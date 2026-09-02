# Hive Metastore (Docker, Postgres-backed, basic auth, MinIO storage)

Self-contained Hive stack: Postgres (metastore DB) + MinIO (S3-compatible
default storage for the warehouse) + Hive Metastore + HiveServer2, all in
Docker. HiveServer2 requires a username/password to connect — no anonymous
access.

## Quick start

```bash
cd hive_metastore
cp .env.example .env
# edit .env: set real POSTGRES_PASSWORD, HIVE_USER/HIVE_PASSWORD, MINIO_ROOT_USER/PASSWORD
docker compose up -d --build
```

That's it — same command works on any machine with Docker installed.

## What's running

| Service      | Port  | Purpose                                          |
|--------------|-------|---------------------------------------------------|
| postgres     | 6432 (host) → 5432 | metastore DB — published for external JDBC-direct clients (see below) |
| minio        | 9000  | S3 API — Hive's default table storage             |
| minio        | 9001  | MinIO web console                                  |
| minio-init   | -     | one-shot: creates the `warehouse` bucket, exits    |
| metastore    | 9083  | Hive Metastore thrift endpoint                    |
| hiveserver2  | 10000 | JDBC/thrift (beeline, JDBC clients, Spark/Yeedu)   |
| hiveserver2  | 10002 | HiveServer2 web UI                                 |

## External client configuration

A generic Hive-compatible engine that supports `CUSTOM` HiveServer2 auth can
point `hive.metastore.uris` at `thrift://<this-host>:9083` directly — it'll
need the same `fs.s3a.*` settings (see `conf/core-site.xml`) to read tables
back.

**Yeedu specifically can't use that path.** Its Hive metastore-catalog
connector only supports `KERBEROS`/`LDAP` for a Thrift connection, and
`UserNamePassword` for a JDBC connection — `CUSTOM` (what this stack's
HiveServer2 uses, via `SimplePasswdAuthenticator`) isn't in its supported
list for either. The connector rejects it outright:

```json
{"error_message": "Unsupported authentication type 'CUSTOM' for connection type 'Thrift'. Supported types: KERBEROS, LDAP (Thrift) and UserNamePassword (JDBC)."}
```

The workaround is Hive's "embedded metastore" pattern: connect straight to
the metastore's backing Postgres DB over JDBC (bypassing the Thrift
metastore/HiveServer2 services entirely), which Yeedu's `UserNamePassword`
JDBC path does support. This requires publishing Postgres to the host (see
the `ports` entry on the `postgres` service — not there by default upstream,
added specifically for this).

`hive-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://<HOST>:6432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value><POSTGRES_USER></value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value><POSTGRES_PASSWORD></value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://warehouse/</value>
  </property>
</configuration>
```

`core-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://<HOST>:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value><MINIO_ROOT_USER></value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value><MINIO_ROOT_PASSWORD></value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

`<HOST>` is the LAN/VPN-reachable address of the machine running this stack.
`<POSTGRES_USER>`/`<POSTGRES_PASSWORD>`/`<MINIO_ROOT_USER>`/`<MINIO_ROOT_PASSWORD>`
come from `hive_metastore/.env` — never commit real values for these.

## Connect (beeline)

```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n "$HIVE_USER" -p "$HIVE_PASSWORD"
```

A wrong username/password is rejected — auth is enforced by
`auth/SimplePasswdAuthenticator.java`, a small `PasswdAuthenticationProvider`
that checks the JDBC credentials against the `HIVE_USER`/`HIVE_PASSWORD` env
vars (see `conf/hive-site.xml`, `hive.server2.authentication=CUSTOM`).

Once connected:

```sql
CREATE TABLE t (id INT);
SHOW TABLES;
DROP TABLE t;
```

`CREATE TABLE` provisions the table's directory under
`s3a://warehouse/default.db/t/` in MinIO — browse it at
`http://localhost:9001` (login with `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`)
or via `mc ls local/warehouse`.

## Spark configuration to write to MinIO

Registering the metastore-catalog (`hive-site.xml`/`core-site.xml` above)
only gets Spark as far as reading/writing *metadata*. Actually writing table
*data* to MinIO needs additional Spark-side config — three separate gaps,
each with its own failure mode if skipped:

1. **`spark.sql.warehouse.dir` must be set explicitly.** Spark deprecated
   reading `hive.metastore.warehouse.dir` from `hive-site.xml` back in Spark
   2.0 — without this, Spark silently defaults to a local
   `./spark-warehouse` directory on the cluster node instead of erroring.
   Must be set **before the Spark session starts** — setting it at runtime
   (e.g. `spark.conf.set(...)` in a running notebook) has no effect; it
   requires a session/kernel restart.

2. **The S3A connector jar isn't on Spark's classpath by default.** Setting
   `fs.s3a.*` properties configures the S3A filesystem's *behavior*, but
   does nothing if the `S3AFileSystem` class was never loaded — that's a
   separate jar (`hadoop-aws` + its `aws-java-sdk-bundle` dependency),
   missing which raises `ClassNotFoundException: Class
   org.apache.hadoop.fs.s3a.S3AFileSystem not found`. Add it as a Maven
   package rather than a manual jar path — this resolves the correct
   transitive `aws-java-sdk-bundle` version automatically and ships to
   executors too (a manually-set `extraClassPath` only covers the driver):
   ```
   org.apache.hadoop:hadoop-aws:3.3.6
   ```
   (Same `hadoop-aws`/`aws-java-sdk-bundle` version pairing as this stack's
   own `Dockerfile`, which symlinks them onto Hive's classpath the same way
   for the same reason.)

3. **The Postgres JDBC driver needs to be on the classpath too**, since
   `hive-site.xml`'s `ConnectionDriverName=org.postgresql.Driver` has to
   actually resolve:
   ```
   spark.driver.extraClassPath  file:///<path-to>/postgresql-42.7.4.jar
   ```

Full Spark config block:

```
spark.sql.warehouse.dir              s3a://warehouse/
spark.hadoop.fs.s3a.impl             org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint         http://<HOST>:9000
spark.hadoop.fs.s3a.access.key       <MINIO_ROOT_USER>
spark.hadoop.fs.s3a.secret.key       <MINIO_ROOT_PASSWORD>
spark.hadoop.fs.s3a.path.style.access       true
spark.hadoop.fs.s3a.connection.ssl.enabled  false
spark.driver.extraClassPath          file:///<path-to>/postgresql-42.7.4.jar
```
plus the `org.apache.hadoop:hadoop-aws:3.3.6` Maven package.

**Gotcha:** a database created *before* `spark.sql.warehouse.dir` was set
keeps its old (local) location permanently in the metastore — new tables in
that database keep inheriting it regardless of later config changes, since
Hive resolves default table location from the database's stored location,
not the current session config. Either recreate the database
(`DROP DATABASE <db> CASCADE; CREATE DATABASE <db>;`) or repoint it
(`ALTER DATABASE <db> SET LOCATION 's3a://warehouse/<db>.db';`, which
affects only tables created after the change).

## Config files

- `conf/hive-site.xml` — metastore JDBC connection (Postgres), HiveServer2
  auth settings, and `hive.metastore.warehouse.dir=s3a://warehouse/`.
  Mounted into both containers via `HIVE_CUSTOM_CONF_DIR` (the image's own
  mechanism for layering custom config over its defaults). Credentials are
  resolved from the container environment at runtime (`${env.POSTGRES_USER}`
  etc.), so `.env` is the only thing to edit.
- `conf/core-site.xml` — `fs.defaultFS=file:///` (no HDFS in this stack) plus
  the `fs.s3a.*` block pointing the S3A connector at the `minio` container
  instead of real AWS (path-style access, no TLS, creds from `.env`).
- `Dockerfile` — multi-stage build: compiles `SimplePasswdAuthenticator` in
  a JDK image (the base `apache/hive:4.0.0` image ships a JRE only), then
  layers the compiled jar, the Postgres JDBC driver, and symlinks to the
  S3A connector jars (`hadoop-aws` + `aws-java-sdk-bundle`, already bundled
  in the base image under Hadoop's optional tools dir, just not on Hive's
  classpath by default) onto the official image.

## Persistence / teardown

Postgres data and MinIO's object data are on named volumes, so data
survives `docker compose restart`/`stop`/`up`. To wipe everything:

```bash
docker compose down -v
```
