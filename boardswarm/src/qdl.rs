use std::collections::HashMap;
use std::str::FromStr;

use bytes::{Bytes, BytesMut};
use qdl::sahara::{SaharaMode, sahara_run};
use qdl::types::{FirehoseConfiguration, FirehoseStorageType, QdlBackend, QdlDevice, QdlReadWrite};
use qdl::{firehose_get_default_sector_size, setup_target_device};
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

#[derive(Deserialize, Debug, Default)]
struct QdlParameters {
    #[serde(rename = "match")]
    match_: HashMap<String, String>,
    /// Storage type of the device: emmc, ufs, nand, nvme or spinor
    storage: Option<String>,
    /// Storage sector size; defaults to the common size for the storage type
    sector_size: Option<usize>,
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
}

#[derive(Debug)]
enum QdlCommand {
    SendProgrammer(Bytes, oneshot::Sender<Result<(), QdlError>>),
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
        reset_on_drop: false,
    })
}

fn send_programmer(
    device: &mut Option<QdlDevice<dyn QdlReadWrite>>,
    parameters: &SessionParameters,
    data: Bytes,
) -> Result<(), QdlError> {
    // The device is only opened once a client actually wants to talk to it, so
    // boardswarm doesn't keep the usb interface claimed while idle
    let device = match device {
        Some(d) => d,
        None => device.insert(open_device(parameters).map_err(|e| QdlError::Open(e.to_string()))?),
    };

    // TODO: qdl can load multi-image cpio programmer archives, but only via
    // load_programmer_images() which takes a path rather then the bytes that
    // got streamed to us. Treat everything as a single image for now.
    let mut images = vec![Some(data.to_vec())];

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
    Ok(())
}

/// Blocking task owning the connection to a device
///
/// qdl is a synchronous library and a session carries state across the Sahara
/// and Firehose phases, so the device is owned by a dedicated thread for as long
/// as the volume is registered rather then being opened per operation.
fn qdl_session(parameters: SessionParameters, mut commands: mpsc::Receiver<QdlCommand>) {
    let mut device = None;
    while let Some(command) = commands.blocking_recv() {
        match command {
            QdlCommand::SendProgrammer(data, tx) => {
                let _ = tx.send(send_programmer(&mut device, &parameters, data));
            }
        }
    }
}

#[derive(Debug)]
struct QdlVolume {
    commands: mpsc::Sender<QdlCommand>,
    targets: [VolumeTargetInfo; 1],
}

impl QdlVolume {
    fn new(parameters: SessionParameters) -> Self {
        let (commands, rx) = mpsc::channel(1);
        std::thread::spawn(move || qdl_session(parameters, rx));

        Self {
            commands,
            targets: [VolumeTargetInfo {
                name: TARGET_PROGRAMMER.to_string(),
                readable: false,
                writable: true,
                seekable: false,
                size: None,
                blocksize: None,
            }],
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
        if target != TARGET_PROGRAMMER {
            return Err(VolumeError::UnknownTargetRequested);
        }

        Ok((
            self.targets[0].clone(),
            Box::new(ProgrammerTarget::new(self.commands.clone(), length)),
        ))
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
    let session = SessionParameters {
        storage,
        sector_size,
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
