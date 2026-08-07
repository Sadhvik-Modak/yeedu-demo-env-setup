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
| postgres     | -     | metastore DB (internal only, not published)       |
| minio        | 9000  | S3 API — Hive's default table storage             |
| minio        | 9001  | MinIO web console                                  |
| minio-init   | -     | one-shot: creates the `warehouse` bucket, exits    |
| metastore    | 9083  | Hive Metastore thrift endpoint                    |
| hiveserver2  | 10000 | JDBC/thrift (beeline, JDBC clients, Spark/Yeedu)   |
| hiveserver2  | 10002 | HiveServer2 web UI                                 |

External engines (e.g. a Spark/Yeedu cluster) that only need the metastore
can point `hive.metastore.uris` at `thrift://<this-host>:9083` directly —
they'll need the same `fs.s3a.*` settings (see `conf/core-site.xml`) to read
tables back.

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
