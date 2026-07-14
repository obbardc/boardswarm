use std::collections::HashMap;
use std::str::FromStr;

use bytes::{Bytes, BytesMut};
use qdl::parsers::{firehose_parser_ack_nak, firehose_parser_configure_response};
use qdl::sahara::{SaharaMode, sahara_run};
use qdl::types::{FirehoseConfiguration, FirehoseStorageType, QdlBackend, QdlDevice, QdlReadWrite};
use qdl::{
    firehose_configure, firehose_get_default_sector_size, firehose_program_storage, firehose_read,
    firehose_read_storage, setup_target_device,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, info, instrument, warn};

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

const USB_VID_QCOM: u64 = 0x05c6;
const USB_PID_EDL: u64 = 0x9008;

/// Maximum size accepted for a programmer image
const PROGRAMMER_MAX_SIZE: usize = 16 * 1024 * 1024;

fn default_luns() -> u8 {
    1
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
    /// Skip initialising the storage; required for unprovisioned media
    #[serde(default)]
    skip_storage_init: bool,
}

impl Default for QdlParameters {
    fn default() -> Self {
        Self {
            match_: HashMap::new(),
            storage: None,
            sector_size: None,
            luns: default_luns(),
            skip_storage_init: false,
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
}

#[derive(Debug)]
enum QdlCommand {
    SendProgrammer(Bytes, oneshot::Sender<Result<(), QdlError>>),
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

fn lun_target(lun: u8, sector_size: usize) -> VolumeTargetInfo {
    VolumeTargetInfo {
        name: lun_target_name(lun),
        readable: true,
        writable: true,
        seekable: true,
        // Firehose can only report storage information through its log output,
        // so the size of a physical partition isn't available
        size: None,
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
    fn new(parameters: SessionParameters) -> Self {
        let (commands, rx) = mpsc::channel(1);
        let sector_size = parameters.sector_size;
        let luns = parameters.luns;
        std::thread::spawn(move || qdl_session(parameters, rx));

        let mut targets = vec![VolumeTargetInfo {
            name: TARGET_PROGRAMMER.to_string(),
            readable: false,
            writable: true,
            seekable: false,
            size: None,
            blocksize: None,
        }];
        targets.extend((0..luns).map(|lun| lun_target(lun, sector_size)));

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
            Box::new(ProgrammerTarget::new(self.commands.clone(), length))
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
        Ok(())
    }
}

struct ProgrammerTarget {
    commands: mpsc::Sender<QdlCommand>,
    data: BytesMut,
}

impl ProgrammerTarget {
    fn new(commands: mpsc::Sender<QdlCommand>, size_hint: Option<u64>) -> Self {
        let data = BytesMut::with_capacity(
            size_hint
                .unwrap_or(1024 * 1024)
                .min(PROGRAMMER_MAX_SIZE as u64) as usize,
        );
        Self { commands, data }
    }

    async fn shutdown(&mut self) -> Result<(), QdlError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(QdlCommand::SendProgrammer(self.data.split().into(), tx))
            .await
            .map_err(|_e| QdlError::SessionGone)?;
        rx.await.map_err(QdlError::NoResponse)?
    }
}

#[async_trait::async_trait]
impl VolumeTarget for ProgrammerTarget {
    async fn write(&mut self, data: Bytes, offset: u64, completion: crate::WriteCompletion) {
        if offset as usize != self.data.len() {
            completion.complete(Err(tonic::Status::out_of_range("Invalid offset")));
        } else if data.len() + self.data.len() > PROGRAMMER_MAX_SIZE {
            completion.complete(Err(tonic::Status::out_of_range("Programmer too big")));
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

    let session = SessionParameters {
        storage,
        sector_size,
        luns: parameters.luns,
        skip_storage_init: parameters.skip_storage_init,
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
                prereg.register_volume(properties, QdlVolume::new(session));
            }
            DeviceEvent::Remove(device) => registrations.remove(&device),
        }
    }
}
