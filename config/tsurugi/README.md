# Tsurugi

Support scope (not covered by CI — validated manually; may break as the driver matures):

- **TPC-C**: working (all five transaction types).
- **YCSB**: working.
- **TATP**: working; point-lookup latency is high pending server-side tuning.
- **TPC-H**: schema and load only. Query execution needs a dialect pass (the parser
  rejects `INTERVAL ?` placeholders, among others), and one compilable query drove the
  server to an out-of-memory kill even at SF=0.01 — do not run the query phase yet.

## Versions

- Tsurugi server >= 1.11.0 (validated on 1.11.2)
- `com.tsurugidb.jdbc:tsurugi-jdbc:0.5.0` (bundles Tsubakuro 1.16.0, matching Tsurugi 1.11.x)

## Server prerequisites

- The TCP endpoint is disabled upstream by default: set `[stream_endpoint] enabled=true`
  in `$TSURUGI_HOME/var/etc/tsurugi.ini` (or connect over IPC with `jdbc:tsurugi:ipc:tsurugi`).
- Keep the server default `sql.lowercase_regular_identifiers=false`: the TPC-C DDL declares
  uppercase column names, which the procedures read back as case-sensitive result labels.
- Quick start with the official container:
  `docker run -d -p 12345:12345 --shm-size=2g ghcr.io/project-tsurugi/tsurugidb`

## URL options in the sample config

- `transactionType=OCC`: TPC-C transactions are short; serialization conflicts surface as
  SQLState 40001 and are retried by the framework.
- `commitType=STORED`: pins durable commit semantics instead of following the server's
  mutable `commit_response` setting, keeping runs reproducible across environments.

## Run

```bash
./mvnw clean package -P tsurugi
tar -xzf target/benchbase-tsurugi.tgz && cd benchbase-tsurugi
java -jar benchbase.jar -b tpcc -c config/tsurugi/sample_tpcc_config.xml \
  --create=true --load=true --execute=true
```
