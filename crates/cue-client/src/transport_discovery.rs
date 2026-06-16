use std::collections::BTreeSet;

use anyhow::Result;

use crate::host_discovery::{HostDiscoveryConfig, detected_configured_hosts};
use crate::ssh_config::detected_ssh_hosts;

pub(crate) fn detected_transport_hosts(
    discovery: &HostDiscoveryConfig,
) -> Result<BTreeSet<String>> {
    let mut hosts = detected_ssh_hosts()?;
    hosts.extend(detected_configured_hosts(discovery)?);
    Ok(hosts)
}
