use std::fmt::Display;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, anyhow, bail};
use boardswarm_client::device::DeviceVolume;
use indicatif::ProgressBar;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use xmltree::{Element, XMLNode};

use crate::utils::BatchWriter;

/// A `<program>` entry from a rawprogram file
#[derive(Debug)]
struct Program {
    label: String,
    filename: String,
    lun: u8,
    sector_size: u64,
    num_sectors: u64,
    start_sector: String,
    file_sector_offset: u64,
}

/// A `<patch>` entry from a patch file
#[derive(Debug)]
struct Patch {
    lun: u8,
    sector_size: u64,
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
        .ok_or_else(|| anyhow!("<{}> without a {name} attribute", e.name))
}

fn attr_parse<T>(e: &Element, name: &str) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    let v = attr(e, name)?;
    v.parse()
        .map_err(|err| anyhow!("<{}> has an invalid {name} of {v:?}: {err}", e.name))
}

fn attr_parse_or<T>(e: &Element, name: &str, default: T) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: Display,
{
    match e.attributes.get(name) {
        Some(v) if !v.is_empty() => attr_parse(e, name),
        _ => Ok(default),
    }
}

fn elements<'a>(xml: &'a Element, name: &str) -> impl Iterator<Item = &'a Element> {
    xml.children.iter().filter_map(move |n| match n {
        XMLNode::Element(e) if e.name.eq_ignore_ascii_case(name) => Some(e),
        _ => None,
    })
}

fn parse_file(path: &Path) -> anyhow::Result<Element> {
    let data = std::fs::read(path).with_context(|| format!("Reading {}", path.display()))?;
    Element::parse(&data[..]).with_context(|| format!("Parsing {}", path.display()))
}

fn parse_programs(path: &Path) -> anyhow::Result<Vec<Program>> {
    programs_from_xml(&parse_file(path)?)
}

fn programs_from_xml(xml: &Element) -> anyhow::Result<Vec<Program>> {
    elements(xml, "program")
        .map(|e| {
            // The server always talks to slot 0
            let slot: u8 = attr_parse_or(e, "slot", 0)?;
            if slot != 0 {
                bail!("<program> for slot {slot}, only slot 0 is supported");
            }
            Ok(Program {
                label: attr(e, "label").unwrap_or("unnamed").to_string(),
                filename: attr(e, "filename")?.to_string(),
                lun: attr_parse(e, "physical_partition_number")?,
                sector_size: attr_parse(e, "SECTOR_SIZE_IN_BYTES")?,
                num_sectors: attr_parse(e, "num_partition_sectors")?,
                start_sector: attr(e, "start_sector")?.to_string(),
                file_sector_offset: attr_parse_or(e, "file_sector_offset", 0)?,
            })
        })
        .collect()
}

fn parse_patches(path: &Path) -> anyhow::Result<Vec<Patch>> {
    patches_from_xml(&parse_file(path)?)
}

fn patches_from_xml(xml: &Element) -> anyhow::Result<Vec<Patch>> {
    elements(xml, "patch")
        .filter(|e| {
            // Patches against anything but the device storage are meant for the
            // files on the host that built the images, and are of no use here.
            // qdl-rs skips these in the same way.
            attr(e, "filename").map(|f| f == "DISK").unwrap_or(false)
        })
        .map(|e| {
            Ok(Patch {
                lun: attr_parse(e, "physical_partition_number")?,
                sector_size: attr_parse(e, "SECTOR_SIZE_IN_BYTES")?,
                start_sector: attr(e, "start_sector")?.to_string(),
                byte_offset: attr_parse(e, "byte_offset")?,
                size_in_bytes: attr_parse(e, "size_in_bytes")?,
                value: attr(e, "value")?.to_string(),
                what: attr(e, "what").unwrap_or("").to_string(),
            })
        })
        .collect()
}

/// Resolve a sector expression from a rawprogram or patch file
///
/// These are either a plain number or an offset from the end of the storage,
/// spelled as `NUM_DISK_SECTORS-33.` with a trailing dot. Firehose has the
/// device evaluate these, but driving it through volume targets means they have
/// to be resolved here, which needs the size of the storage.
fn eval_sectors(expr: &str, num_disk_sectors: Option<u64>) -> anyhow::Result<u64> {
    let e = expr.trim().trim_end_matches('.').trim();
    if let Ok(v) = e.parse::<u64>() {
        return Ok(v);
    }

    if e.starts_with("CRC32") {
        bail!(
            "{expr} needs the device to compute a checksum over its own storage, \
             which can't be expressed through a volume target"
        );
    }

    let rest = e
        .strip_prefix("NUM_DISK_SECTORS")
        .ok_or_else(|| anyhow!("Unsupported expression {expr:?}"))?
        .trim();
    let num_disk_sectors = num_disk_sectors.ok_or_else(|| {
        anyhow!(
            "{expr} needs the size of the storage, which the server doesn't know; \
             set lun_sizes in the qdl provider configuration"
        )
    })?;
    if rest.is_empty() {
        return Ok(num_disk_sectors);
    }

    let (op, n) = rest.split_at(1);
    let n: u64 = n
        .trim()
        .parse()
        .map_err(|e| anyhow!("Unsupported expression {expr:?}: {e}"))?;
    match op {
        "-" => num_disk_sectors
            .checked_sub(n)
            .ok_or_else(|| anyhow!("{expr} is before the start of the storage")),
        "+" => num_disk_sectors
            .checked_add(n)
            .ok_or_else(|| anyhow!("{expr} overflows")),
        _ => bail!("Unsupported operator in {expr:?}"),
    }
}

/// Check a target lines up with what the file expects it to be
fn check_target(
    target: &str,
    io: &boardswarm_client::client::VolumeIoRW,
    sector_size: u64,
) -> anyhow::Result<Option<u64>> {
    match io.blocksize() {
        Some(b) if b as u64 == sector_size => (),
        Some(b) => bail!(
            "{target} has a sector size of {b} bytes, but the file expects {sector_size}; \
             check sector_size in the qdl provider configuration"
        ),
        None => bail!("{target} doesn't report a sector size"),
    }
    Ok(io.size().map(|s| s / sector_size))
}

async fn write_program(volume: &mut DeviceVolume, dir: &Path, p: &Program) -> anyhow::Result<()> {
    let path = dir.join(&p.filename);
    let mut f = tokio::fs::File::open(&path)
        .await
        .with_context(|| format!("Opening {}", path.display()))?;

    let len = p.num_sectors * p.sector_size;
    let target = format!("lun{}", p.lun);
    let mut io = volume
        .open(&target, Some(len))
        .await
        .with_context(|| format!("Opening {target}"))?;

    let num_disk_sectors = check_target(&target, &io, p.sector_size)?;
    let start = eval_sectors(&p.start_sector, num_disk_sectors)
        .with_context(|| format!("Resolving where {} goes", p.label))?;

    io.seek(SeekFrom::Start(start * p.sector_size)).await?;
    f.seek(SeekFrom::Start(p.file_sector_offset * p.sector_size))
        .await?;

    println!(
        "Writing {} to {target} sector {start} ({len} bytes)",
        p.label
    );
    let progress = ProgressBar::new(len);
    let mut writer = BatchWriter::new(io).discard_flush();
    let mut wrapped = progress.wrap_async_write(&mut writer);
    // The file can hold more then this entry wants, and a file shorter then the
    // sectors it covers gets zero padded by the server
    tokio::io::copy(&mut (&mut f).take(len), &mut wrapped).await?;
    wrapped.shutdown().await.context("Volume shutdown")?;
    progress.finish_and_clear();

    Ok(())
}

async fn apply_patch(volume: &mut DeviceVolume, p: &Patch) -> anyhow::Result<()> {
    if p.size_in_bytes == 0 || p.size_in_bytes > 8 {
        bail!("<patch> of {} bytes is not supported", p.size_in_bytes);
    }

    let target = format!("lun{}", p.lun);
    let mut io = volume
        .open(&target, None)
        .await
        .with_context(|| format!("Opening {target}"))?;
    let num_disk_sectors = check_target(&target, &io, p.sector_size)?;

    let start = eval_sectors(&p.start_sector, num_disk_sectors)?;
    let value = eval_sectors(&p.value, num_disk_sectors)
        .with_context(|| format!("Resolving the value to patch in for {:?}", p.what))?;

    // Firehose patches in place; through a volume target the sector holding the
    // value has to be read, modified and written back
    let at = start * p.sector_size + p.byte_offset;
    let sector = at / p.sector_size;
    let in_sector = (at % p.sector_size) as usize;
    let size = p.size_in_bytes as usize;
    if in_sector + size > p.sector_size as usize {
        bail!("<patch> for {:?} straddles a sector boundary", p.what);
    }

    io.seek(SeekFrom::Start(sector * p.sector_size)).await?;
    let mut buf = vec![0u8; p.sector_size as usize];
    io.read_exact(&mut buf)
        .await
        .with_context(|| format!("Reading {target} sector {sector}"))?;

    buf[in_sector..in_sector + size].copy_from_slice(&value.to_le_bytes()[..size]);

    io.seek(SeekFrom::Start(sector * p.sector_size)).await?;
    io.write_all(&buf)
        .await
        .with_context(|| format!("Writing {target} sector {sector}"))?;
    io.shutdown().await.context("Volume shutdown")?;

    println!("Patched {target} sector {sector}: {}", p.what);
    Ok(())
}

pub async fn flash(
    volume: &mut DeviceVolume,
    programs: &[PathBuf],
    patches: &[PathBuf],
) -> anyhow::Result<()> {
    // Parse everything before touching the device, so a typo in the last file
    // doesn't leave a half flashed board behind
    let mut to_program = Vec::new();
    for path in programs {
        let dir = path
            .parent()
            .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?
            .to_path_buf();
        for p in parse_programs(path)? {
            // Rawprogram files carry placeholder entries for partitions that
            // aren't being flashed
            if p.filename.is_empty() || p.num_sectors == 0 {
                continue;
            }
            let file = dir.join(&p.filename);
            if !file.exists() {
                bail!(
                    "{} needs {}, which doesn't exist",
                    path.display(),
                    file.display()
                );
            }
            to_program.push((dir.clone(), p));
        }
    }

    let mut to_patch = Vec::new();
    for path in patches {
        to_patch.extend(parse_patches(path)?);
    }

    if to_program.is_empty() && to_patch.is_empty() {
        bail!("Nothing to flash");
    }

    for (dir, p) in &to_program {
        write_program(volume, dir, p).await?;
    }
    for p in &to_patch {
        apply_patch(volume, p).await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::{eval_sectors, patches_from_xml, programs_from_xml};
    use xmltree::Element;

    const RAWPROGRAM: &str = r#"<?xml version="1.0" ?>
<data>
  <program SECTOR_SIZE_IN_BYTES="4096" file_sector_offset="0" filename="gpt_main0.bin" label="PrimaryGPT" num_partition_sectors="6" physical_partition_number="0" size_in_KB="24.0" sparse="false" start_byte_hex="0x0" start_sector="0"/>
  <program SECTOR_SIZE_IN_BYTES="4096" file_sector_offset="2" filename="xbl.elf" label="xbl_a" num_partition_sectors="928" physical_partition_number="1" size_in_KB="3712.0" sparse="false" start_byte_hex="0x6000" start_sector="6"/>
  <program SECTOR_SIZE_IN_BYTES="4096" file_sector_offset="0" filename="" label="empty" num_partition_sectors="0" physical_partition_number="0" size_in_KB="0.0" sparse="false" start_byte_hex="0x0" start_sector="100"/>
  <program SECTOR_SIZE_IN_BYTES="4096" file_sector_offset="0" filename="gpt_backup0.bin" label="BackupGPT" num_partition_sectors="5" physical_partition_number="0" size_in_KB="20.0" sparse="false" start_byte_hex="0x0" start_sector="NUM_DISK_SECTORS-5."/>
</data>"#;

    const PATCHFILE: &str = r#"<?xml version="1.0" ?>
<patches>
  <patch SECTOR_SIZE_IN_BYTES="4096" byte_offset="24" filename="DISK" physical_partition_number="0" size_in_bytes="4" start_sector="1" value="NUM_DISK_SECTORS-1." what="Update Backup Header Location"/>
  <patch SECTOR_SIZE_IN_BYTES="4096" byte_offset="0" filename="rawprogram0.xml" physical_partition_number="0" size_in_bytes="4" start_sector="0" value="12345" what="Host side patch"/>
</patches>"#;

    #[test]
    fn parses_rawprogram_entries() {
        let programs = programs_from_xml(&Element::parse(RAWPROGRAM.as_bytes()).unwrap()).unwrap();
        assert_eq!(programs.len(), 4);

        let gpt = &programs[0];
        assert_eq!(gpt.label, "PrimaryGPT");
        assert_eq!(gpt.filename, "gpt_main0.bin");
        assert_eq!(gpt.lun, 0);
        assert_eq!(gpt.sector_size, 4096);
        assert_eq!(gpt.num_sectors, 6);
        assert_eq!(gpt.start_sector, "0");

        // physical_partition_number picks the target, and file_sector_offset
        // skips over part of the file
        assert_eq!(programs[1].lun, 1);
        assert_eq!(programs[1].file_sector_offset, 2);

        // Placeholder entries are kept by the parser and skipped when flashing
        assert_eq!(programs[2].filename, "");
        assert_eq!(programs[2].num_sectors, 0);

        // The backup table is placed relative to the end of the storage
        assert_eq!(programs[3].start_sector, "NUM_DISK_SECTORS-5.");
    }

    #[test]
    fn skips_host_side_patches() {
        let patches = patches_from_xml(&Element::parse(PATCHFILE.as_bytes()).unwrap()).unwrap();
        // Only the DISK patch applies to the device
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].byte_offset, 24);
        assert_eq!(patches[0].size_in_bytes, 4);
        assert_eq!(patches[0].value, "NUM_DISK_SECTORS-1.");
    }

    #[test]
    fn plain_sector_numbers() {
        assert_eq!(eval_sectors("0", None).unwrap(), 0);
        assert_eq!(eval_sectors("1234", None).unwrap(), 1234);
        // Size is only needed for expressions that refer to it
        assert_eq!(eval_sectors("6", None).unwrap(), 6);
    }

    #[test]
    fn relative_to_end_of_disk() {
        // The trailing dot is how these are spelled in rawprogram files
        assert_eq!(eval_sectors("NUM_DISK_SECTORS-33.", Some(100)).unwrap(), 67);
        assert_eq!(eval_sectors("NUM_DISK_SECTORS-1.", Some(100)).unwrap(), 99);
        assert_eq!(eval_sectors("NUM_DISK_SECTORS", Some(100)).unwrap(), 100);
        assert_eq!(eval_sectors("NUM_DISK_SECTORS+2.", Some(100)).unwrap(), 102);
    }

    #[test]
    fn needs_the_disk_size() {
        assert!(eval_sectors("NUM_DISK_SECTORS-33.", None).is_err());
    }

    #[test]
    fn rejects_what_it_cannot_evaluate() {
        // The device computes these over its own storage
        assert!(eval_sectors("CRC32(2,16384)", Some(100)).is_err());
        assert!(eval_sectors("NUM_DISK_SECTORS-101.", Some(100)).is_err());
        assert!(eval_sectors("whatever", Some(100)).is_err());
    }
}
