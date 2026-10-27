//! Garage: the single-node config file and key-pair variables it starts
//! from, and the admin API port its readiness path is on.

use testcontainers::{ContainerRequest, GenericImage, ImageExt};

use super::{ACCESS_KEY, REGION, SECRET_KEY};

/// The container port of the S3 API, as [`config`] binds it.
pub(super) const PORT: u16 = 3900;

/// The container port of the admin API, as [`config`] binds it. Garage
/// refuses every unsigned S3 request, so readiness is polled here.
pub(super) const ADMIN_PORT: u16 = 3903;

/// A path the admin API answers `200` without credentials once the node is
/// up. Garage applies the single-node layout and imports the key before
/// either API binds, so a signed call succeeds from the first `200`.
pub(super) const READY_PATH: &str = "/health";

/// Where Garage reads its config file when given none.
const CONFIG_PATH: &str = "/etc/garage.toml";

/// The config of a one-node cluster serving the S3 API for [`REGION`].
/// `--single-node` requires `replication_factor = 1`. Every address binds
/// `0.0.0.0` so the host reaches the mapped ports.
pub(super) fn config() -> String {
    // The rpc_secret is a fixed test value; no other node ever joins.
    format!(
        "metadata_dir = \"/var/lib/garage/meta\"\n\
         data_dir = \"/var/lib/garage/data\"\n\
         replication_factor = 1\n\
         consistency_mode = \"consistent\"\n\
         rpc_bind_addr = \"0.0.0.0:3901\"\n\
         rpc_secret = \"{rpc_secret}\"\n\
         \n\
         [s3_api]\n\
         s3_region = \"{REGION}\"\n\
         api_bind_addr = \"0.0.0.0:{PORT}\"\n\
         \n\
         [admin]\n\
         api_bind_addr = \"0.0.0.0:{ADMIN_PORT}\"\n",
        rpc_secret = "0".repeat(64),
    )
}

/// The image running one node from [`config`] with the fixed key pair
/// imported. The image holds only the binary, so there is no script.
pub(super) fn configure(image: GenericImage) -> ContainerRequest<GenericImage> {
    image
        .with_copy_to(CONFIG_PATH, config().into_bytes())
        .with_env_var("GARAGE_DEFAULT_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("GARAGE_DEFAULT_SECRET_KEY", SECRET_KEY)
        .with_cmd(["/garage", "server", "--single-node", "--default-access-key"])
}
