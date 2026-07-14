use std::collections::HashMap;
use std::str::FromStr;

use bytes::{Bytes, BytesMut};
use qdl::parsers::{firehose_parser_ack_nak, firehose_parser_configure_response};
use qdl::sahara::{SaharaMode, sahara_run};
use qdl::types::{
    FirehoseConfiguration, FirehoseResetMode, FirehoseStorageType, QdlBackend, QdlDevice,
    QdlReadWrite,
};
use qdl::{
    firehose_configure, firehose_get_default_sector_size, firehose_patch, firehose_program_storage,
    firehose_read, firehose_read_storage, firehose_reset, firehose_set_bootable,
    setup_target_device,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, info, instrument, warn};
use xmltree::{Element, XMLNode};

use crate::{
    Server, Volume, VolumeError, VolumeTarget, VolumeTargetInfo, registry,
    udev::{DeviceEvent, DeviceRegistrations},
};

pub const PROVIDER: &str = "qdl";

/// Target to upload the Firehose programmer to.
///
/// The device only accepts this while it is still in Sahara mode, which is the
/// state it enters EDL in. Once the programmer is running the device switches to
/// the Firehose protocol and this target can no longer be used.
pub const TARGET_PROGRAMMER: &str = "programmer";

/// Target to apply firehose patches to the storage through.
///
/// Patch operations refer to values only the device can work out, like the size
/// of its storage or a checksum over part of it. They carry no data of their
/// own, so rather then expressing them through the lun targets, a patch file is
/// written here and the operations in it are handed to the device as they are.
pub const TARGET_PATCH: &str = "patch";

const USB_VID_QCOM: u64 = 0x05c6;
const USB_PID_EDL: u64 = 0x9008;

/// Maximum size accepted for a programmer image
const PROGRAMMER_MAX_SIZE: usize = 16 * 1024 * 1024;

/// Maximum size accepted for a patch file
const PATCH_MAX_SIZE: usize = 1024 * 1024;

fn default_luns() -> u8 {
    1
}

/// What a device should do when a volume gets committed
///
/// [FirehoseResetMode] can't be used directly as it isn't copyable and doesn't
/// deserialize.
#[derive(Deserialize, Debug, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
enum ResetMode {
    /// Reboot back into EDL, leaving the device ready for another flash
    #[default]
    Edl,
    /// Reboot into the system that was just flashed
    System,
    /// Power the device off
    Off,
}

impl From<ResetMode> for FirehoseResetMode {
    fn from(mode: ResetMode) -> Self {
        match mode {
            ResetMode::Edl => FirehoseResetMode::ResetToEdl,
            ResetMode::System => FirehoseResetMode::Reset,
            ResetMode::Off => FirehoseResetMode::Off,
        }
    }
}

#[derive(Deserialize, Debug)]
struct QdlParameters {
    #[serde(rename = "match")]
    #[serde(default)]
    match_: HashMap<String, String>,
    /// Storage type of the device: emmc, ufs, nand, nvme or spinor
    storage: Option<String>,
    /// Storage sector size; defaults to the common size for the storage type
    sector_size: Option<usize>,
    /// Number of physical storage partitions to expose; ufs devices typically
    /// have several luns, other storage types only have one
    #[serde(default = "default_luns")]
    luns: u8,
    /// Size of each physical storage partition in bytes, in lun order
    ///
    /// Clients need this to work out where the end of a lun is, which is where
    /// a backup partition table goes.
    // TODO: the <getstorageinfo> response carries this, but qdl only prints it
    // to the firehose log and drops it. Parsing it upstream would make this
    // configuration unnecessary and remove the risk of it being wrong.
    #[serde(default)]
    lun_sizes: Vec<u64>,
    /// Skip initialising the storage; required for unprovisioned media
    #[serde(default)]
    skip_storage_init: bool,
    /// What committing the volume should make the device do
    #[serde(default)]
    reset_mode: ResetMode,
    /// Physical storage partition holding the bootloader, marked as bootable
    /// when the volume gets committed
    bootable_lun: Option<u8>,
}

impl Default for QdlParameters {
    fn default() -> Self {
        Self {
            match_: HashMap::new(),
            storage: None,
            sector_size: None,
            luns: default_luns(),
            lun_sizes: Vec::new(),
            skip_storage_init: false,
            reset_mode: ResetMode::default(),
            bootable_lun: None,
        }
    }
}

#[derive(Debug, Error)]
enum QdlError {
    #[error("qdl session is gone")]
    SessionGone,
    #[error("qdl session did not respond: {0}")]
    NoResponse(oneshot::error::RecvError),
    #[error("Failed to open device: {0}")]
    Open(String),
    #[error("qdl failure: {0}")]
    Failure(String),
}

impl From<QdlError> for VolumeError {
    fn from(e: QdlError) -> Self {
        match e {
            QdlError::SessionGone | QdlError::NoResponse(_) => VolumeError::Internal(e.to_string()),
            QdlError::Open(_) | QdlError::Failure(_) => VolumeError::Failure(e.to_string()),
        }
    }
}

impl From<QdlError> for tonic::Status {
    fn from(e: QdlError) -> Self {
        VolumeError::from(e).into()
    }
}

/// Configuration needed to set up a Firehose channel to a device
#[derive(Debug, Clone, Copy)]
struct SessionParameters {
    storage: FirehoseStorageType,
    sector_size: usize,
    luns: u8,
    skip_storage_init: bool,
    reset_mode: ResetMode,
    bootable_lun: Option<u8>,
}

/// A `<patch>` operation from a patch file
#[derive(Debug)]
struct PatchEntry {
    lun: u8,
    start_sector: String,
    byte_offset: u64,
    size_in_bytes: u64,
    value: String,
    what: String,
}

fn attr<'a>(e: &'a Element, name: &str) -> anyhow::Result<&'a str> {
    e.attributes
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| anyhow::anyhow!("<{}> without a {name} attribute", e.name))
}

fn attr_parse<T>(e: &Element, name: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let v = attr(e, name)?;
    v.parse()
        .map_err(|err| anyhow::anyhow!("<{}> has an invalid {name} of {v:?}: {err}", e.name))
}

/// Pick the operations out of a patch file that apply to the device
///
/// The start_sector and value of an operation are handed to the device as they
/// are: they're expressions it evaluates itself, referring to things like the
/// size of its storage or a checksum over part of it.
fn parse_patches(data: &[u8], sector_size: usize) -> anyhow::Result<Vec<PatchEntry>> {
    let xml = Element::parse(data)?;
    let mut entries = Vec::new();

    for node in &xml.children {
        let XMLNode::Element(e) = node else { continue };
        if !e.name.eq_ignore_ascii_case("patch") {
            continue;
        }
        // Patches against a file rather then the storage are meant for the host
        // that built the images and are of no use here; qdl-rs skips these too
        if attr(e, "filename")? != "DISK" {
            continue;
        }

        let s: usize = attr_parse(e, "SECTOR_SIZE_IN_BYTES")?;
        if s != sector_size {
            anyhow::bail!(
                "<patch> uses a sector size of {s} bytes but the volume is configured for {sector_size}"
            );
        }
        let slot: u8 = match e.attributes.get("slot") {
            Some(s) => s.parse()?,
            None => 0,
        };
        if slot != 0 {
            anyhow::bail!("<patch> for slot {slot}, only slot 0 is supported");
        }

        entries.push(PatchEntry {
            lun: attr_parse(e, "physical_partition_number")?,
            start_sector: attr(e, "start_sector")?.to_string(),
            byte_offset: attr_parse(e, "byte_offset")?,
            size_in_bytes: attr_parse(e, "size_in_bytes")?,
            value: attr(e, "value")?.to_string(),
            what: attr(e, "what").unwrap_or("").to_string(),
        });
    }

    Ok(entries)
}

#[derive(Debug)]
enum QdlCommand {
    SendProgrammer(Bytes, oneshot::Sender<Result<(), QdlError>>),
    Patch(Bytes, oneshot::Sender<Result<(), QdlError>>),
    Program {
        lun: u8,
        start_sector: u64,
        data: Bytes,
        tx: oneshot::Sender<Result<(), QdlError>>,
    },
    Read {
        lun: u8,
        start_sector: u32,
        num_sectors: usize,
        tx: oneshot::Sender<Result<Bytes, QdlError>>,
    },
    Commit(oneshot::Sender<Result<(), QdlError>>),
}

fn open_device(
    parameters: &SessionParameters,
) -> Result<QdlDevice<dyn QdlReadWrite>, anyhow::Error> {
    // TODO: qdl always opens the first device it finds in EDL mode, so it can
    // pick a different device then the one udev told us about. This means only
    // one device in EDL mode can be handled at a time; fixing this needs an
    // upstream API to open a device by its usb bus number and address.
    let rw = setup_target_device(QdlBackend::Usb, None, None)?;

    Ok(QdlDevice {
        rw,
        fh_cfg: FirehoseConfiguration {
            storage_type: parameters.storage,
            storage_sector_size: parameters.sector_size,
            backend: QdlBackend::Usb,
            // qdl defaults this to true, which makes the device accept storage
            // writes and then quietly drop them
            bypass_storage: false,
            // The remaining values get overwritten by the <configure> handshake
            ..Default::default()
        },
        // qdl-rs resets the device when a session is dropped after an error;
        // leave that to the client here, which can reset through the device
        // modes like it would for any other volume
        reset_on_drop: false,
    })
}

/// Connection to a single device in EDL mode
///
/// A device enters EDL speaking Sahara, which is only used to upload the
/// Firehose programmer. Once that programmer runs, the same usb connection
/// switches over to Firehose and storage becomes accessible; there is no way
/// back other then resetting the device.
struct Session {
    parameters: SessionParameters,
    device: Option<QdlDevice<dyn QdlReadWrite>>,
    firehose: bool,
}

impl Session {
    fn new(parameters: SessionParameters) -> Self {
        Self {
            parameters,
            device: None,
            firehose: false,
        }
    }

    /// The device is only opened once a client actually wants to talk to it, so
    /// boardswarm doesn't keep the usb interface claimed while idle
    fn device(&mut self) -> Result<&mut QdlDevice<dyn QdlReadWrite>, QdlError> {
        if self.device.is_none() {
            self.device =
                Some(open_device(&self.parameters).map_err(|e| QdlError::Open(e.to_string()))?);
        }
        Ok(self.device.as_mut().unwrap())
    }

    /// Bring up Firehose after the programmer got started by Sahara
    fn start_firehose(&mut self) -> Result<(), QdlError> {
        let skip_storage_init = self.parameters.skip_storage_init;
        let device = self.device()?;

        let mut setup = || -> anyhow::Result<()> {
            // The programmer announces itself with a set of log messages
            firehose_read(device, firehose_parser_ack_nak)?;
            firehose_configure(device, skip_storage_init)?;
            // Picks up the buffer sizes the device is willing to accept
            firehose_read(device, firehose_parser_configure_response)?;
            Ok(())
        };
        setup().map_err(|e| QdlError::Failure(e.to_string()))?;

        self.firehose = true;
        info!("Firehose configured");
        Ok(())
    }

    fn send_programmer(&mut self, data: Bytes) -> Result<(), QdlError> {
        if self.firehose {
            return Err(QdlError::Failure(
                "Programmer is already running; reset the device to upload a new one".to_string(),
            ));
        }

        // TODO: qdl can load multi-image cpio programmer archives, but only via
        // load_programmer_images() which takes a path rather then the bytes that
        // got streamed to us. Treat everything as a single image for now.
        let mut images = vec![Some(data.to_vec())];

        let device = self.device()?;
        sahara_run(
            device,
            SaharaMode::WaitingForImage,
            None,
            &mut images,
            vec![],
            false,
        )
        .map_err(|e| QdlError::Failure(e.to_string()))?;
        info!("Programmer uploaded");

        self.start_firehose()
    }

    fn require_firehose(&self) -> Result<(), QdlError> {
        if self.firehose {
            Ok(())
        } else {
            Err(QdlError::Failure(format!(
                "No programmer running; write one to the {TARGET_PROGRAMMER} target first"
            )))
        }
    }

    fn program(&mut self, lun: u8, start_sector: u64, data: Bytes) -> Result<(), QdlError> {
        self.require_firehose()?;

        // Firehose only deals in whole sectors, so a trailing partial sector
        // gets zero padded; this is also what qdl does for files that don't end
        // on a sector boundary.
        let sector_size = self.parameters.sector_size;
        let num_sectors = data.len().div_ceil(sector_size);
        let mut buf = data.to_vec();
        buf.resize(num_sectors * sector_size, 0);

        let label = format!("lun{lun}");
        let device = self.device()?;
        firehose_program_storage(
            device,
            &mut std::io::Cursor::new(&buf),
            &label,
            num_sectors,
            0,
            lun,
            &start_sector.to_string(),
        )
        .map_err(|e| QdlError::Failure(e.to_string()))
    }

    fn read(&mut self, lun: u8, start_sector: u32, num_sectors: usize) -> Result<Bytes, QdlError> {
        self.require_firehose()?;

        let sector_size = self.parameters.sector_size;
        let mut buf = Vec::with_capacity(num_sectors * sector_size);
        let device = self.device()?;
        firehose_read_storage(device, &mut buf, num_sectors, 0, lun, start_sector)
            .map_err(|e| QdlError::Failure(e.to_string()))?;

        Ok(buf.into())
    }

    /// Apply the operations in a patch file to the storage
    fn patch(&mut self, data: Bytes) -> Result<(), QdlError> {
        self.require_firehose()?;

        let entries = parse_patches(&data, self.parameters.sector_size)
            .map_err(|e| QdlError::Failure(e.to_string()))?;
        let device = self.device()?;

        for e in &entries {
            info!("Patching lun{}: {}", e.lun, e.what);
            firehose_patch(
                device,
                e.byte_offset,
                0,
                e.lun,
                e.size_in_bytes,
                &e.start_sector,
                &e.value,
            )
            .map_err(|err| QdlError::Failure(format!("Patch {:?} failed: {err}", e.what)))?;
        }

        info!("Applied {} patches", entries.len());
        Ok(())
    }

    /// Finish the session off by resetting the device
    fn commit(&mut self) -> Result<(), QdlError> {
        self.require_firehose()?;

        let bootable_lun = self.parameters.bootable_lun;
        let mode = FirehoseResetMode::from(self.parameters.reset_mode);
        let device = self.device()?;

        if let Some(lun) = bootable_lun {
            info!("Marking lun{lun} as bootable");
            firehose_set_bootable(device, lun).map_err(|e| QdlError::Failure(e.to_string()))?;
        }

        info!("Resetting device to {mode}");
        let r = firehose_reset(device, &mode, 0).map_err(|e| QdlError::Failure(e.to_string()));

        // The device is on its way out regardless of how the reset went, so drop
        // the connection to it. It'll come back as a new volume through udev if
        // it re-enters EDL.
        self.device = None;
        self.firehose = false;

        r
    }
}

/// Blocking task owning the connection to a device
///
/// qdl is a synchronous library and a session carries state across the Sahara
/// and Firehose phases, so the device is owned by a dedicated thread for as long
/// as the volume is registered rather then being opened per operation.
fn qdl_session(parameters: SessionParameters, mut commands: mpsc::Receiver<QdlCommand>) {
    let mut session = Session::new(parameters);
    while let Some(command) = commands.blocking_recv() {
        match command {
            QdlCommand::SendProgrammer(data, tx) => {
                let _ = tx.send(session.send_programmer(data));
            }
            QdlCommand::Patch(data, tx) => {
                let _ = tx.send(session.patch(data));
            }
            QdlCommand::Program {
                lun,
                start_sector,
                data,
                tx,
            } => {
                let _ = tx.send(session.program(lun, start_sector, data));
            }
            QdlCommand::Read {
                lun,
                start_sector,
                num_sectors,
                tx,
            } => {
                let _ = tx.send(session.read(lun, start_sector, num_sectors));
            }
            QdlCommand::Commit(tx) => {
                let _ = tx.send(session.commit());
            }
        }
    }
}

/// Name of the target giving raw access to a physical storage partition
///
/// Firehose addresses storage by physical partition (a LUN on UFS) and sector,
/// so each one is exposed as a seekable target rather then mapping out the
/// partitions in it. A flash typically starts by writing the partition table,
/// so there isn't necessarily one to look partition names up in.
const LUN_TARGET_PREFIX: &str = "lun";

fn lun_target_name(lun: u8) -> String {
    format!("{LUN_TARGET_PREFIX}{lun}")
}

fn lun_from_target_name(target: &str) -> Option<u8> {
    target.strip_prefix(LUN_TARGET_PREFIX)?.parse().ok()
}

fn lun_target(lun: u8, sector_size: usize, size: Option<u64>) -> VolumeTargetInfo {
    VolumeTargetInfo {
        name: lun_target_name(lun),
        readable: true,
        writable: true,
        seekable: true,
        // Firehose can only report storage information through its log output,
        // so this is only known if it was configured
        size,
        blocksize: Some(sector_size as u32),
    }
}

#[derive(Debug)]
struct QdlVolume {
    commands: mpsc::Sender<QdlCommand>,
    targets: Vec<VolumeTargetInfo>,
    sector_size: usize,
}

impl QdlVolume {
    fn new(parameters: SessionParameters, lun_sizes: &[u64]) -> Self {
        let (commands, rx) = mpsc::channel(1);
        let sector_size = parameters.sector_size;
        let luns = parameters.luns;
        std::thread::spawn(move || qdl_session(parameters, rx));

        let mut targets = vec![
            VolumeTargetInfo {
                name: TARGET_PROGRAMMER.to_string(),
                readable: false,
                writable: true,
                seekable: false,
                size: None,
                blocksize: None,
            },
            VolumeTargetInfo {
                name: TARGET_PATCH.to_string(),
                readable: false,
                writable: true,
                seekable: false,
                size: None,
                blocksize: None,
            },
        ];
        targets.extend(
            (0..luns).map(|lun| lun_target(lun, sector_size, lun_sizes.get(lun as usize).copied())),
        );

        Self {
            commands,
            targets,
            sector_size,
        }
    }
}

#[async_trait::async_trait]
impl Volume for QdlVolume {
    fn targets(&self) -> (&[VolumeTargetInfo], bool) {
        (&self.targets, true)
    }

    async fn open(
        &self,
        target: &str,
        length: Option<u64>,
    ) -> Result<(VolumeTargetInfo, Box<dyn VolumeTarget>), VolumeError> {
        let Some(info) = self.targets.iter().find(|t| t.name == target) else {
            return Err(VolumeError::UnknownTargetRequested);
        };

        let t: Box<dyn VolumeTarget> = if target == TARGET_PROGRAMMER {
            Box::new(BufferedTarget::new(
                self.commands.clone(),
                length,
                PROGRAMMER_MAX_SIZE,
                QdlCommand::SendProgrammer,
            ))
        } else if target == TARGET_PATCH {
            Box::new(BufferedTarget::new(
                self.commands.clone(),
                length,
                PATCH_MAX_SIZE,
                QdlCommand::Patch,
            ))
        } else {
            let lun = lun_from_target_name(target).ok_or(VolumeError::UnknownTargetRequested)?;
            Box::new(LunTarget {
                commands: self.commands.clone(),
                lun,
                sector_size: self.sector_size,
            })
        };

        Ok((info.clone(), t))
    }

    async fn commit(&self) -> Result<(), VolumeError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(QdlCommand::Commit(tx))
            .await
            .map_err(|_e| QdlError::SessionGone)?;
        rx.await.map_err(QdlError::NoResponse)??;
        Ok(())
    }
}

/// Target that collects everything written to it and hands it to the session as
/// one command on shutdown
struct BufferedTarget {
    commands: mpsc::Sender<QdlCommand>,
    data: BytesMut,
    max: usize,
    command: fn(Bytes, oneshot::Sender<Result<(), QdlError>>) -> QdlCommand,
}

impl BufferedTarget {
    fn new(
        commands: mpsc::Sender<QdlCommand>,
        size_hint: Option<u64>,
        max: usize,
        command: fn(Bytes, oneshot::Sender<Result<(), QdlError>>) -> QdlCommand,
    ) -> Self {
        let data =
            BytesMut::with_capacity(size_hint.unwrap_or(1024 * 1024).min(max as u64) as usize);
        Self {
            commands,
            data,
            max,
            command,
        }
    }

    async fn shutdown(&mut self) -> Result<(), QdlError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send((self.command)(self.data.split().into(), tx))
            .await
            .map_err(|_e| QdlError::SessionGone)?;
        rx.await.map_err(QdlError::NoResponse)?
    }
}

#[async_trait::async_trait]
impl VolumeTarget for BufferedTarget {
    async fn write(&mut self, data: Bytes, offset: u64, completion: crate::WriteCompletion) {
        if offset as usize != self.data.len() {
            completion.complete(Err(tonic::Status::out_of_range("Invalid offset")));
        } else if data.len() + self.data.len() > self.max {
            completion.complete(Err(tonic::Status::out_of_range("Too much data for target")));
        } else {
            self.data.extend_from_slice(&data);
            completion.complete(Ok(data.len() as u64));
        }
    }

    async fn shutdown(&mut self, completion: crate::ShutdownCompletion) {
        completion.complete(self.shutdown().await.map_err(Into::into));
    }
}

/// Raw access to one physical storage partition
///
/// Every write turns into a complete Firehose <program> operation, so writes
/// have to start on a sector boundary; a client should follow the advertised
/// blocksize. A trailing partial sector is zero padded, which means writing a
/// sector twice at different offsets within it does not merge.
struct LunTarget {
    commands: mpsc::Sender<QdlCommand>,
    lun: u8,
    sector_size: usize,
}

impl LunTarget {
    fn start_sector(&self, offset: u64) -> Result<u64, tonic::Status> {
        if !offset.is_multiple_of(self.sector_size as u64) {
            return Err(tonic::Status::out_of_range(format!(
                "Offset {offset} is not a multiple of the {} byte sector size",
                self.sector_size
            )));
        }
        Ok(offset / self.sector_size as u64)
    }

    async fn do_write(&mut self, data: Bytes, offset: u64) -> Result<u64, tonic::Status> {
        let start_sector = self.start_sector(offset)?;
        let len = data.len() as u64;

        let (tx, rx) = oneshot::channel();
        self.commands
            .send(QdlCommand::Program {
                lun: self.lun,
                start_sector,
                data,
                tx,
            })
            .await
            .map_err(|_e| QdlError::SessionGone)?;
        rx.await.map_err(QdlError::NoResponse)??;

        Ok(len)
    }

    async fn do_read(&mut self, length: u64, offset: u64) -> Result<Bytes, tonic::Status> {
        let start_sector = self
            .start_sector(offset)?
            .try_into()
            .map_err(|_e| tonic::Status::out_of_range("Offset too big for firehose"))?;
        // Firehose reads whole sectors; a partial one gets trimmed off again
        let num_sectors = (length as usize).div_ceil(self.sector_size);

        let (tx, rx) = oneshot::channel();
        self.commands
            .send(QdlCommand::Read {
                lun: self.lun,
                start_sector,
                num_sectors,
                tx,
            })
            .await
            .map_err(|_e| QdlError::SessionGone)?;
        let mut data = rx.await.map_err(QdlError::NoResponse)??;

        data.truncate(length as usize);
        Ok(data)
    }
}

#[async_trait::async_trait]
impl VolumeTarget for LunTarget {
    async fn read(&mut self, length: u64, offset: u64, completion: crate::ReadCompletion) {
        completion.complete(self.do_read(length, offset).await);
    }

    async fn write(&mut self, data: Bytes, offset: u64, completion: crate::WriteCompletion) {
        completion.complete(self.do_write(data, offset).await);
    }
}

#[instrument(skip(server, parameters))]
pub async fn start_provider(name: String, parameters: Option<serde_yaml::Value>, server: Server) {
    let parameters: QdlParameters = match parameters {
        Some(p) => match serde_yaml::from_value(p) {
            Ok(p) => p,
            Err(e) => {
                warn!("Invalid qdl provider parameters: {e}");
                return;
            }
        },
        None => Default::default(),
    };

    let storage = match parameters.storage.as_deref() {
        Some(s) => match FirehoseStorageType::from_str(s) {
            Ok(s) => s,
            Err(_) => {
                warn!("Unknown qdl storage type: {s}");
                return;
            }
        },
        None => FirehoseStorageType::Emmc,
    };
    let sector_size = match parameters.sector_size {
        Some(s) => s,
        None => match firehose_get_default_sector_size(&storage.to_string()) {
            Some(s) => s,
            None => {
                warn!("No default sector size for storage type {storage}");
                return;
            }
        },
    };
    if sector_size == 0 {
        warn!("qdl sector size can't be zero");
        return;
    }
    // qdl asserts that its send buffer is a multiple of the sector size while
    // configuring the device; catch a bad sector size here rather then having
    // the session thread panic halfway through a flash
    let send_buffer = FirehoseConfiguration::default().send_buffer_size;
    if !send_buffer.is_multiple_of(sector_size) {
        warn!("qdl sector size {sector_size} doesn't divide the send buffer size {send_buffer}");
        return;
    }

    if let Some(lun) = parameters.bootable_lun
        && lun >= parameters.luns
    {
        warn!(
            "qdl bootable_lun {lun} is outside of the {} configured luns",
            parameters.luns
        );
        return;
    }

    // A wrong lun size puts a backup partition table in the wrong place, which
    // isn't something a client can notice, so be strict about it here
    if !parameters.lun_sizes.is_empty() {
        if parameters.lun_sizes.len() != parameters.luns as usize {
            warn!(
                "qdl lun_sizes has {} entries but {} luns are configured",
                parameters.lun_sizes.len(),
                parameters.luns
            );
            return;
        }
        if let Some((lun, size)) = parameters
            .lun_sizes
            .iter()
            .enumerate()
            .find(|(_, size)| **size == 0 || !size.is_multiple_of(sector_size as u64))
        {
            warn!(
                "qdl lun{lun} size {size} is not a non-zero multiple of the {sector_size} byte sector size"
            );
            return;
        }
    }

    let session = SessionParameters {
        storage,
        sector_size,
        luns: parameters.luns,
        skip_storage_init: parameters.skip_storage_init,
        reset_mode: parameters.reset_mode,
        bootable_lun: parameters.bootable_lun,
    };

    let registrations = DeviceRegistrations::new(server);
    let provider_properties = &[
        (registry::PROVIDER_NAME, name.as_str()),
        (registry::PROVIDER, PROVIDER),
    ];
    let mut devices = crate::udev::DeviceStream::new("usb").unwrap();
    while let Some(d) = devices.next().await {
        match d {
            DeviceEvent::Add { device, seqnum } => {
                if device.property_u64("ID_VENDOR_ID", 16) != Some(USB_VID_QCOM)
                    || device.property_u64("ID_MODEL_ID", 16) != Some(USB_PID_EDL)
                {
                    continue;
                }

                let Some(busnum) = device.property_u64("BUSNUM", 10) else {
                    continue;
                };
                let Some(devnum) = device.property_u64("DEVNUM", 10) else {
                    continue;
                };

                let name = format!("qdl {busnum}/{devnum}");
                let mut properties = device.properties(&name);
                if !properties.matches(&parameters.match_) {
                    debug!(
                        "Ignoring qdl device {} - {:?}",
                        device.syspath().display(),
                        properties
                    );
                    continue;
                }

                info!("New qdl volume: {name}");
                properties.extend(provider_properties);

                // Unlike other providers the device doesn't get probed before
                // registering: the Sahara handshake can only be done once and
                // reading it here would break the following programmer upload.
                let prereg = registrations.pre_register(&device, seqnum);
                prereg.register_volume(properties, QdlVolume::new(session, &parameters.lun_sizes));
            }
            DeviceEvent::Remove(device) => registrations.remove(&device),
        }
    }
}

#[cfg(test)]
mod test {
    use super::parse_patches;

    const PATCHFILE: &[u8] = br#"<?xml version="1.0" ?>
<patches>
  <patch SECTOR_SIZE_IN_BYTES="4096" byte_offset="24" filename="DISK" physical_partition_number="0" size_in_bytes="4" start_sector="1" value="NUM_DISK_SECTORS-1." what="Update Backup Header Location"/>
  <patch SECTOR_SIZE_IN_BYTES="4096" byte_offset="88" filename="DISK" physical_partition_number="2" size_in_bytes="4" start_sector="1" value="CRC32(2,16384)" what="Update Partition Entry Array CRC"/>
  <patch SECTOR_SIZE_IN_BYTES="4096" byte_offset="0" filename="rawprogram0.xml" physical_partition_number="0" size_in_bytes="4" start_sector="0" value="12345" what="Host side patch"/>
</patches>"#;

    #[test]
    fn parses_patches_for_the_device() {
        let patches = parse_patches(PATCHFILE, 4096).unwrap();
        // The patch against rawprogram0.xml is for the host that built the
        // images, not for the device
        assert_eq!(patches.len(), 2);

        assert_eq!(patches[0].lun, 0);
        assert_eq!(patches[0].byte_offset, 24);
        assert_eq!(patches[0].size_in_bytes, 4);
        // Expressions are handed to the device untouched
        assert_eq!(patches[0].start_sector, "1");
        assert_eq!(patches[0].value, "NUM_DISK_SECTORS-1.");

        assert_eq!(patches[1].lun, 2);
        assert_eq!(patches[1].value, "CRC32(2,16384)");
    }

    #[test]
    fn rejects_a_mismatched_sector_size() {
        // Applying these against the wrong sector size would patch the wrong
        // place entirely
        assert!(parse_patches(PATCHFILE, 512).is_err());
    }
}
