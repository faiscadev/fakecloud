mod helpers;

use helpers::TestServer;
use tokio_postgres::NoTls;

async fn connect_postgres_with_retry(
    host: &str,
    port: i32,
    user: &str,
    password: &str,
    dbname: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let connection_string =
        format!("host={host} port={port} user={user} password={password} dbname={dbname}");
    let mut last_error = None;
    for _ in 0..40 {
        match tokio_postgres::connect(&connection_string, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(conn);
                return Ok(client);
            }
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
    Err(last_error.expect("postgres connection error"))
}

/// DB instances survive a restart. Reproduces issue #914: a DB instance
/// created with `aws rds create-db-instance` disappeared after restart
/// because the background container-start task flipped status to
/// `available` without persisting, and the load path then dropped the
/// row as a "stuck creating" placeholder.
///
/// Uses `start_full` rather than `start_persistent` because the latter
/// disables the container CLI (most persistence tests cover metadata-
/// only ops); CreateDBInstance needs real Docker.
#[tokio::test]
async fn persistence_round_trip_db_instance() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let extra_args = ["--storage-mode", "persistent", "--data-path", &data_path];
    let mut server = TestServer::start_full(&[], &extra_args).await;
    let client = server.rds_client().await;

    client
        .create_db_instance()
        .db_instance_identifier("persist-db")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .unwrap();

    let _ = helpers::wait_for_db_available(&client, "persist-db", 180).await;

    drop(client);
    server.restart().await;
    let client = server.rds_client().await;

    // Container recovery is async (#1338): the row reloads as
    // `starting`, then flips back to `available` once the container is
    // healthy. Wait rather than asserting an instantaneous status.
    let inst = helpers::wait_for_db_available(&client, "persist-db", 240).await;
    assert_eq!(inst.db_instance_identifier(), Some("persist-db"));
    assert_eq!(inst.engine(), Some("postgres"));
    assert_eq!(inst.master_username(), Some("admin"));
    assert_eq!(inst.db_instance_status(), Some("available"));
}

/// Backing container is recreated on restart so the persisted DB
/// endpoint is actually usable. Reproduces issue #1338: pre-fix,
/// `DescribeDBInstances` returned `available` but `tokio_postgres::connect`
/// failed because the container was gone.
#[tokio::test]
async fn persistence_db_endpoint_works_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let extra_args = ["--storage-mode", "persistent", "--data-path", &data_path];
    let mut server = TestServer::start_full(&[], &extra_args).await;
    let client = server.rds_client().await;

    client
        .create_db_instance()
        .db_instance_identifier("restart-db")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .unwrap();
    let _ = helpers::wait_for_db_available(&client, "restart-db", 240).await;

    drop(client);
    server.restart().await;
    let client = server.rds_client().await;

    let inst = helpers::wait_for_db_available(&client, "restart-db", 240).await;
    let endpoint = inst.endpoint().expect("endpoint");
    let host = endpoint.address().expect("address");
    let port = endpoint.port().expect("port");

    let db = connect_postgres_with_retry(host, port, "admin", "secret123", "appdb")
        .await
        .expect("connect to recovered postgres container");
    let row = db.query_one("SELECT 1", &[]).await.expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

/// StopDBInstance and StartDBInstance actually toggle the backing
/// container, not just emit events. Pre-fix the ops were XML stubs and
/// the container kept running regardless of the reported status.
#[tokio::test]
async fn start_stop_db_instance_toggles_backing_container() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let extra_args = ["--storage-mode", "persistent", "--data-path", &data_path];
    let server = TestServer::start_full(&[], &extra_args).await;
    let client = server.rds_client().await;

    client
        .create_db_instance()
        .db_instance_identifier("toggle-db")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .unwrap();
    let inst = helpers::wait_for_db_available(&client, "toggle-db", 240).await;
    let endpoint = inst.endpoint().expect("endpoint");
    let host = endpoint.address().expect("address").to_string();
    let port_before = endpoint.port().expect("port");

    // Sanity: endpoint works before stopping.
    let _ = connect_postgres_with_retry(&host, port_before, "admin", "secret123", "appdb")
        .await
        .expect("connect before stop");

    client
        .stop_db_instance()
        .db_instance_identifier("toggle-db")
        .send()
        .await
        .unwrap();

    let stopped = client
        .describe_db_instances()
        .db_instance_identifier("toggle-db")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stopped.db_instances()[0].db_instance_status(),
        Some("stopped"),
    );
    // Endpoint must not be reachable now.
    let connect = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio_postgres::connect(
            &format!("host={host} port={port_before} user=admin password=secret123 dbname=appdb"),
            NoTls,
        ),
    )
    .await;
    assert!(
        matches!(connect, Err(_) | Ok(Err(_))),
        "stopped DB endpoint should not accept connections",
    );

    client
        .start_db_instance()
        .db_instance_identifier("toggle-db")
        .send()
        .await
        .unwrap();
    let restarted = helpers::wait_for_db_available(&client, "toggle-db", 240).await;
    let endpoint = restarted.endpoint().expect("endpoint");
    let host_after = endpoint.address().expect("address");
    let port_after = endpoint.port().expect("port");

    let db = connect_postgres_with_retry(host_after, port_after, "admin", "secret123", "appdb")
        .await
        .expect("connect after start");
    let row = db.query_one("SELECT 1", &[]).await.expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

/// Data WRITTEN to a DB survives a restart, not just the endpoint. With
/// persistent mode now defaulting DB data volumes on, a row inserted before
/// restart is still readable after the backing container is recreated. Without
/// the durable volume the recovered postgres container would come back empty
/// and the row would be gone -- this is the data-plane half of persistence.
#[tokio::test]
async fn persistence_db_row_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let extra_args = ["--storage-mode", "persistent", "--data-path", &data_path];
    let mut server = TestServer::start_full(&[], &extra_args).await;
    let client = server.rds_client().await;

    client
        .create_db_instance()
        .db_instance_identifier("rowsurvive-db")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .unwrap();
    let inst = helpers::wait_for_db_available(&client, "rowsurvive-db", 240).await;
    let endpoint = inst.endpoint().expect("endpoint");
    let host = endpoint.address().expect("address").to_string();
    let port = endpoint.port().expect("port");

    // Seed a row before the restart.
    {
        let db = connect_postgres_with_retry(&host, port, "admin", "secret123", "appdb")
            .await
            .expect("connect to seed");
        db.batch_execute(
            "CREATE TABLE durable (id int primary key, note text); \
             INSERT INTO durable (id, note) VALUES (1, 'survives-restart');",
        )
        .await
        .expect("seed row");
    }

    drop(client);
    server.restart().await;
    let client = server.rds_client().await;

    let inst = helpers::wait_for_db_available(&client, "rowsurvive-db", 240).await;
    let endpoint = inst.endpoint().expect("endpoint");
    let host = endpoint.address().expect("address").to_string();
    let port = endpoint.port().expect("port");

    let db = connect_postgres_with_retry(&host, port, "admin", "secret123", "appdb")
        .await
        .expect("connect to recovered container");
    let row = db
        .query_one("SELECT note FROM durable WHERE id = 1", &[])
        .await
        .expect("row must survive the restart via the durable data volume");
    let note: String = row.get(0);
    assert_eq!(note, "survives-restart");
}

/// Parameter groups survive a restart.
#[tokio::test]
async fn persistence_round_trip_parameter_group() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.rds_client().await;

    client
        .create_db_parameter_group()
        .db_parameter_group_name("persist-pg")
        .db_parameter_group_family("postgres16")
        .description("Persistence test parameter group")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.rds_client().await;

    let groups = client
        .describe_db_parameter_groups()
        .db_parameter_group_name("persist-pg")
        .send()
        .await
        .unwrap();
    let pgs = groups.db_parameter_groups();
    assert!(
        pgs.iter()
            .any(|g| g.db_parameter_group_name() == Some("persist-pg")),
        "parameter group should survive restart"
    );
    let pg = pgs
        .iter()
        .find(|g| g.db_parameter_group_name() == Some("persist-pg"))
        .unwrap();
    assert_eq!(pg.db_parameter_group_family(), Some("postgres16"));
    assert_eq!(pg.description(), Some("Persistence test parameter group"));
}

/// Subnet groups survive a restart.
#[tokio::test]
async fn persistence_round_trip_subnet_group() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.rds_client().await;

    client
        .create_db_subnet_group()
        .db_subnet_group_name("persist-subnet-grp")
        .db_subnet_group_description("Persistence test subnet group")
        .subnet_ids("subnet-aaa")
        .subnet_ids("subnet-bbb")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.rds_client().await;

    let groups = client
        .describe_db_subnet_groups()
        .db_subnet_group_name("persist-subnet-grp")
        .send()
        .await
        .unwrap();
    let sgs = groups.db_subnet_groups();
    assert_eq!(sgs.len(), 1);
    let sg = &sgs[0];
    assert_eq!(sg.db_subnet_group_name(), Some("persist-subnet-grp"));
    assert_eq!(
        sg.db_subnet_group_description(),
        Some("Persistence test subnet group")
    );
    let subnet_ids: Vec<&str> = sg
        .subnets()
        .iter()
        .filter_map(|s| s.subnet_identifier())
        .collect();
    assert!(subnet_ids.contains(&"subnet-aaa"));
    assert!(subnet_ids.contains(&"subnet-bbb"));
}

/// Deletion survives a restart: a deleted parameter group does not reappear.
#[tokio::test]
async fn persistence_deletion_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.rds_client().await;

    client
        .create_db_parameter_group()
        .db_parameter_group_name("doomed-pg")
        .db_parameter_group_family("postgres16")
        .description("Will be deleted")
        .send()
        .await
        .unwrap();

    client
        .delete_db_parameter_group()
        .db_parameter_group_name("doomed-pg")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.rds_client().await;

    let groups = client.describe_db_parameter_groups().send().await.unwrap();
    assert!(
        !groups
            .db_parameter_groups()
            .iter()
            .any(|g| g.db_parameter_group_name() == Some("doomed-pg")),
        "deleted parameter group should not reappear"
    );
}
