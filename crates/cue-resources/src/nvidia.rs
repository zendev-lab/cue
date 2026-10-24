use crate::{Attempt, Grant, Needs, Quantity};
use anyhow::{Context, Result, bail};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Device {
    pub uuid: String,
    pub total_bytes: u64,
    pub free_bytes: u64,
}

/// nvidia-smi uses NVML and is loaded only for a configured NVIDIA provider.
pub async fn probe(argv: &[String]) -> Result<Vec<Device>> {
    let mut args = argv.to_vec();
    args.extend([
        "--query-gpu=uuid,memory.total,memory.free".into(),
        "--format=csv,noheader,nounits".into(),
    ]);
    parse(&String::from_utf8(
        crate::command::run(&args, Vec::new(), 3_000).await?,
    )?)
}
fn parse(text: &str) -> Result<Vec<Device>> {
    let mut devices = Vec::new();
    for line in text.lines().filter(|s| !s.trim().is_empty()) {
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 3 || !fields[0].starts_with("GPU-") {
            bail!("invalid NVIDIA capacity response")
        }
        let bytes = |s: &str| -> Result<u64> {
            s.parse::<u64>()?
                .checked_mul(1 << 20)
                .context("NVIDIA byte count overflow")
        };
        let device = Device {
            uuid: fields[0].into(),
            total_bytes: bytes(fields[1])?,
            free_bytes: bytes(fields[2])?,
        };
        if device.free_bytes > device.total_bytes
            || devices.iter().any(|d: &Device| d.uuid == device.uuid)
        {
            bail!("inconsistent NVIDIA device snapshot")
        }
        devices.push(device);
    }
    devices.sort_by(|a, b| a.uuid.cmp(&b.uuid));
    if devices.is_empty() {
        bail!("no NVIDIA devices available")
    }
    Ok(devices)
}

pub fn select(
    devices: &[Device],
    needs: &Needs,
    held: &[Attempt],
    margin: u64,
    id: &str,
) -> Result<Option<Grant>> {
    let count = needs.get("gpu").map(|q| q.value()).unwrap_or(1);
    let bytes = needs.get("gpu_mem").map(|q| q.value());
    let mut selected = Vec::new();
    for device in devices {
        let mut reserved = 0u64;
        let mut exclusive = false;
        for attempt in held {
            if attempt.released {
                continue;
            }
            if attempt
                .grant
                .as_ref()
                .is_some_and(|g| g.devices.contains(&device.uuid))
            {
                match attempt.request.needs.get("gpu_mem") {
                    Some(Quantity::Bytes(n)) => reserved = reserved.saturating_add(*n),
                    _ => exclusive = true,
                }
            }
        }
        let free = device
            .free_bytes
            .min(device.total_bytes.saturating_sub(reserved))
            .saturating_sub(margin);
        if !exclusive && bytes.map_or(reserved == 0, |n| free >= n) {
            selected.push(device.uuid.clone());
            if selected.len() as u64 == count {
                break;
            }
        }
    }
    if selected.len() as u64 != count {
        return Ok(None);
    }
    Ok(Some(Grant {
        id: id.into(),
        environment: std::collections::BTreeMap::from([
            ("CUDA_VISIBLE_DEVICES".into(), selected.join(",")),
            ("CUDA_DEVICE_ORDER".into(), "PCI_BUS_ID".into()),
        ]),
        devices: selected,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reservations_and_physical_free_are_not_double_counted_and_exclusive_is_respected() {
        use crate::{Backend, ProviderConfig, ProviderRequest};
        let devices = parse("GPU-a, 100, 40\n").unwrap();
        let mut held = Attempt {
            provider: ProviderConfig {
                id: "gpu".into(),
                backend: Backend::Nvidia {
                    argv: vec!["nvidia-smi".into()],
                    safety_margin_bytes: 0,
                },
            },
            request: ProviderRequest {
                version: 1,
                method: "reserve".into(),
                daemon_id: "daemon".into(),
                request_id: "old".into(),
                execution: cue_core::ExecutionId(1),
                needs: Needs::from([("gpu_mem".into(), Quantity::Bytes(60 << 20))]),
            },
            grant: Some(Grant {
                id: "old".into(),
                environment: Default::default(),
                devices: vec!["GPU-a".into()],
            }),
            uncertain: false,
            released: false,
        };
        let needs = Needs::from([("gpu_mem".into(), Quantity::Bytes(40 << 20))]);
        assert!(
            select(&devices, &needs, &[held.clone()], 0, "new")
                .unwrap()
                .is_some()
        );
        assert!(
            select(&devices, &needs, &[held.clone()], 1, "new")
                .unwrap()
                .is_none()
        );
        assert!(
            select(
                &devices,
                &Needs::from([("gpu".into(), Quantity::Count(1))]),
                &[held.clone()],
                0,
                "new"
            )
            .unwrap()
            .is_none()
        );
        held.request.needs = Needs::from([("gpu".into(), Quantity::Count(1))]);
        assert!(
            select(&devices, &needs, &[held.clone()], 0, "new")
                .unwrap()
                .is_none()
        );
        held.released = true;
        assert!(
            select(&devices, &needs, &[held], 0, "new")
                .unwrap()
                .is_some()
        );
        assert!(parse("GPU-a, 100, 101").is_err());
        assert!(parse("GPU-a, 100, 50\nGPU-a, 100, 50").is_err());
    }
    #[test]
    fn selection_uses_stable_identity_and_per_device_budget() {
        let devices = parse("GPU-b, 100, 30\nGPU-a, 100, 80\n").unwrap();
        let needs = Needs::from([
            ("gpu".into(), Quantity::Count(2)),
            ("gpu_mem".into(), Quantity::Bytes(24 << 20)),
        ]);
        let grant = select(&devices, &needs, &[], 0, "req").unwrap().unwrap();
        assert_eq!(grant.devices, ["GPU-a", "GPU-b"]);
        assert_eq!(grant.environment["CUDA_VISIBLE_DEVICES"], "GPU-a,GPU-b");
        assert!(
            select(&devices, &needs, &[], 8 << 20, "req")
                .unwrap()
                .is_none()
        );
    }
}
