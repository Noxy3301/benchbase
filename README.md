# BenchBase

[![BenchBase (Java with Maven)](https://github.com/cmu-db/benchbase/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/cmu-db/benchbase/actions/workflows/maven.yml)

> [!NOTE]
> **`research/helios` branch** — patched for [Noxy3301/helios](https://github.com/Noxy3301/helios).
>
> Changes from upstream `main`:
> - **OCC commit-conflict retry** (`api/Worker.java`, `tpcc/TPCCLoader.java`) — `Worker.isRetryable()` recognizes MySQL error 1180 / `HY000` carrying `"Got error 149"` (`HA_ERR_LOCK_DEADLOCK`) and `SQLTransactionRollbackException` with SQLState `40001` as transient, and the TPC-C loader re-sends a conflicting batch through its `BatchWriter` under full-jitter backoff, halving a batch that keeps losing validation.
> - **TPC-C prefetch plan injection** (`tpcc/procedures/*.java`) — when `HELIOS_PREFETCH_PLAN=1` (or `-Dhelios.prefetchPlan=true`), each procedure builds a Helios DSL plan via `SET SESSION helios_tx_plan` and the index-sensitive SQLStmts append `FORCE INDEX (...)` hints to keep MySQL's access path aligned with the DSL plan; otherwise SQL falls back to the upstream form.
> - **Parallel TPC-C data load** (`tpcc/TPCCLoader.java`, `tpcc/TPCCUtil.java`) — ITEM splits by id range and each warehouse into stock, customer+history and new_order+order_line units, latched parent-before-child along the DDL foreign keys and queued children-after-parents so a FIFO pool of any size stays deadlock-free. OORDER stays at one writer per warehouse. The shard budget is `availableProcessors()` clamped to `1..1024` (`-Dtpcc.load.shards=N` to override) and is spread across warehouses, and each unit derives its generator from `(randomSeed, table, warehouse, chunk)`, so RNG-derived values do not depend on pool scheduling.
> - **Parallel TPC-H data load** (`tpch/TPCHLoader.java`) — `createLoaderThreads()` shards PART, CUSTOMER, ORDERS, PARTSUPP and LINEITEM into `N = availableProcessors()` loader threads (`-Dtpch.load.shards=N` to override) over the generators' deterministic `(part, partCount)` chunks, so N shards produce the same dataset as one below scale factor 30000 (see the warning below); REGION, NATION and SUPPLIER stay single-sharded. A `CountDownLatch` DAG sized to each parent's shard count keeps FK parent-before-child order on a successful load, and `genTable()` rethrows a failed batch so the process aborts rather than reporting a load that ran against a partial parent.
> - **Parallel worker connection setup** (`api/BenchmarkModule.java`, `api/Worker.java`) — connection setup moves out of the `Worker` constructor into `openConnection()`, which `makeWorkers()` drives through a bounded pool. Workers are still constructed serially, so worker ids, construction order and RNG capture are unchanged; only the handshakes overlap.
> - **Per-workload autocommit mode** (`api/Worker.java`, `WorkloadConfiguration.java`, `DBWorkload.java`) — `<autocommit>true</autocommit>` runs every request on an autocommit connection: `setAutoCommit(false)` is skipped and commit/rollback become no-ops, so a multi-statement procedure is no longer atomic. MySQL refuses secondary-engine offload for a statement inside a multi-statement transaction, so read-only analytic configs (TPC-H) set it to reach the columnar engine; transactional workloads leave it unset.
> - **Shared Zipfian zeta constant** (`ycsb/*.java`, `distributions/ZipfianGenerator.java`) — zeta depends only on (item count, theta), so `makeWorkersImpl()` computes it once and passes it to every worker through the precomputed-zetan constructor. Generator fields and drawn sequences are unchanged for a given seed.
> - **Tsurugi support** (`benchmarks/*`, `resources/benchmarks/*`, `config/tsurugi/`) — a `tsurugi` Maven profile, DDL for TPC-C, TATP, YCSB and TPC-H plus a TPC-C dialect file, prepared statements cached per `SQLStmt` and invalidated when the connection changes, and DECIMAL binds rounded to the declared column scale, which strict-typing drivers require. TPC-C, YCSB and TATP run; TPC-H covers schema and load only. See `config/tsurugi/README.md`.

> [!WARNING]
> At TPC-H scale factor 30000 and above, a sharded load produces a **different dataset**
> than a single-shard load. ORDERS and LINEITEM switch their key generators to
> `RowRandomLong`, whose sequential draws use a 64-bit LCG while `advanceRows()` skips
> ahead with 32-bit constants, so every shard after the first resumes a stream the
> single-shard generator never visits. Load with `-Dtpch.load.shards=1` at those scale
> factors. This predates the parallel load and also affects upstream `RowRandomLong`.

BenchBase (formerly [OLTPBench](https://github.com/oltpbenchmark/oltpbench/)) is a Multi-DBMS SQL Benchmarking Framework via JDBC.

**Table of Contents**

- [Quickstart](#quickstart)
- [Description](#description)
- [Usage Guide](#usage-guide)
- [Contributing](#contributing)
- [Known Issues](#known-issues)
- [Credits](#credits)
- [Citing This Repository](#citing-this-repository)

---

## Quickstart

To clone and build BenchBase using the `postgres` profile,

```bash
git clone --depth 1 https://github.com/cmu-db/benchbase.git
cd benchbase
./mvnw clean package -P postgres
```

This produces artifacts in the `target` folder, which can be extracted,

```bash
cd target
tar xvzf benchbase-postgres.tgz
cd benchbase-postgres
```

Inside this folder, you can run BenchBase. For example, to execute the `tpcc` benchmark,

```bash
java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
```

A full list of options can be displayed,

```bash
java -jar benchbase.jar -h
```

---

## Description

Benchmarking is incredibly useful, yet endlessly painful. This benchmark suite is the result of a group of
PhDs/post-docs/professors getting together and combining their workloads/frameworks/experiences/efforts. We hope this
will save other people's time, and will provide an extensible platform, that can be grown in an open-source fashion.

BenchBase is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

The BenchBase framework has the following benchmarks:

* [AuctionMark](https://github.com/cmu-db/benchbase/wiki/AuctionMark)
* [CH-benCHmark](https://github.com/cmu-db/benchbase/wiki/CH-benCHmark)
* [Epinions.com](https://github.com/cmu-db/benchbase/wiki/epinions)
* hyadapt -- pending configuration files
* [NoOp](https://github.com/cmu-db/benchbase/wiki/NoOp)
* [OT-Metrics](https://github.com/cmu-db/benchbase/wiki/OT-Metrics)
* [Resource Stresser](https://github.com/cmu-db/benchbase/wiki/Resource-Stresser)
* [SEATS](https://github.com/cmu-db/benchbase/wiki/Seats)
* [SIBench](https://github.com/cmu-db/benchbase/wiki/SIBench)
* [SmallBank](https://github.com/cmu-db/benchbase/wiki/SmallBank)
* [TATP](https://github.com/cmu-db/benchbase/wiki/TATP)
* [TPC-C](https://github.com/cmu-db/benchbase/wiki/TPC-C)
* [TPC-H](https://github.com/cmu-db/benchbase/wiki/TPC-H)
* TPC-DS -- pending configuration files
* [Twitter](https://github.com/cmu-db/benchbase/wiki/Twitter)
* [Voter](https://github.com/cmu-db/benchbase/wiki/Voter)
* [Wikipedia](https://github.com/cmu-db/benchbase/wiki/Wikipedia)
* [YCSB](https://github.com/cmu-db/benchbase/wiki/YCSB)

This framework is design to allow for easy extension. We provide stub code that a contributor can use to include a new
benchmark, leveraging all the system features (logging, controlled speed, controlled mixture, etc.)

---

## Usage Guide

### How to Build
Run the following command to build the distribution for a given database specified as the profile name (`-P`).  The following profiles are currently supported: `postgres`, `mysql`, `mariadb`, `sqlite`, `cockroachdb`, `phoenix`, `spanner`, and `tsurugi`.

```bash
./mvnw clean package -P <profile name>
```

The following files will be placed in the `./target` folder:

* `benchbase-<profile name>.tgz`
* `benchbase-<profile name>.zip`

### How to Run
Once you build and unpack the distribution, you can run `benchbase` just like any other executable jar.  The following examples assume you are running from the root of the expanded `.zip` or `.tgz` distribution.  If you attempt to run `benchbase` outside of the distribution structure you may encounter a variety of errors including `java.lang.NoClassDefFoundError`.

To bring up help contents:
```bash
java -jar benchbase.jar -h
```

To execute the `tpcc` benchmark:
```bash
java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
```

For composite benchmarks like `chbenchmark`, which require multiple schemas to be created and loaded, you can provide a comma separated list:
```bash
java -jar benchbase.jar -b tpcc,chbenchmark -c config/postgres/sample_chbenchmark_config.xml --create=true --load=true --execute=true
```

The following options are provided:

```text
usage: benchbase
 -b,--bench <arg>               [required] Benchmark class. Currently
                                supported: [tpcc, tpch, tatp, wikipedia,
                                resourcestresser, twitter, epinions, ycsb,
                                seats, auctionmark, chbenchmark, voter,
                                sibench, noop, smallbank, hyadapt,
                                otmetrics, templated]
 -c,--config <arg>              [required] Workload configuration file
    --clear <arg>               Clear all records in the database for this
                                benchmark
    --create <arg>              Initialize the database for this benchmark
 -d,--directory <arg>           Base directory for the result files,
                                default is current directory
    --dialects-export <arg>     Export benchmark SQL to a dialects file
    --execute <arg>             Execute the benchmark workload
 -h,--help                      Print this help
 -im,--interval-monitor <arg>   Throughput Monitoring Interval in
                                milliseconds
 -jh,--json-histograms <arg>    Export histograms to JSON file
    --load <arg>                Load data using the benchmark's data
                                loader
 -s,--sample <arg>              Sampling window
```

### How to Run with Maven

Instead of first building, packaging and extracting before running benchbase, it is possible to execute benchmarks directly against the source code using Maven. Once you have the project cloned you can run any benchmark from the root project directory using the Maven `exec:java` goal. For example, the following command executes the `tpcc` benchmark against `postgres`:

```
mvn clean compile exec:java -P postgres -Dexec.args="-b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true"
```

this is equivalent to the steps above but eliminates the need to first package and then extract the distribution.

### How to Enable Logging

To enable logging, e.g., for the PostgreSQL JDBC driver, add the following JVM property when starting...

```
-Djava.util.logging.config.file=src/main/resources/logging.properties
```

To modify the logging level you can update [`logging.properties`](src/main/resources/logging.properties) and/or [`log4j.properties`](src/main/resources/log4j.properties).

### How to Release

```
./mvnw -B release:prepare
./mvnw -B release:perform
```

### How use with Docker

- Build or pull a dev image to help building from source:

  ```sh
  ./docker/benchbase/build-dev-image.sh
  ./docker/benchbase/run-dev-image.sh
  ```

  or

  ```sh
  docker run -it --rm --pull \
    -v /path/to/benchbase-source:/benchbase \
    -v $HOME/.m2:/home/containeruser/.m2 \
    benchbase.azure.cr.io/benchbase-dev
  ```

- Build the full image:

  ```sh
  # build an image with all profiles
  ./docker/benchbase/build-full-image.sh

  # or if you only want to build some of them
  BENCHBASE_PROFILES='postgres mysql' ./docker/benchbase/build-full-image.sh
  ```

- Run the image for a given profile:

  ```sh
  BENCHBASE_PROFILE='postgres' ./docker/benchbase/run-full-image.sh --help # or other benchbase args as before
  ```

  or

  ```sh
  docker run -it --rm --env BENCHBASE_PROFILE='postgres' \
    -v results:/benchbase/results benchbase.azurecr.io/benchbase --help # or other benchbase args as before
  ```

> See the [docker/benchbase/README.md](./docker/benchbase/) for further details.

[Github Codespaces](https://github.com/features/codespaces) and [VSCode devcontainer](https://code.visualstudio.com/docs/remote/containers) support is also available.

### How to Add Support for a New Database

Please see the existing MySQL and PostgreSQL code for an example.

---

## Contributing

We welcome all contributions! Please open a [pull request](https://github.com/cmu-db/benchbase/pulls). Common contributions may include:

- Adding support for a new DBMS.
- Adding more tests of existing benchmarks.
- Fixing any bugs or known issues.

Please see the [CONTRIBUTING.md](./CONTRIBUTING.md) for addition notes.

## Known Issues

Please use [GitHub's issue tracker](https://github.com/cmu-db/benchbase/issues) for all issues.

## Credits

BenchBase is the official modernized version of the original OLTPBench.

The original OLTPBench code was largely written by the authors of the original paper, [OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf), D. E. Difallah, A. Pavlo, C. Curino, and P. Cudré-Mauroux. In VLDB 2014. Please see the citation guide below.

A significant portion of the modernization was contributed by [Tim Veil @ Cockroach Labs](https://github.com/timveil-cockroach), including but not limited to:

* Built with and for Java ~~17~~ 21.
* Migration from Ant to Maven.
  * Reorganized project to fit Maven structure.
  * Removed static `lib` directory and dependencies.
  * Updated required dependencies and removed unused or unwanted dependencies.
  * Moved all non `.java` files to standard Maven `resources` directory.
  * Shipped with [Maven Wrapper](https://maven.apache.org/wrapper).
* Improved packaging and versioning.
    * Moved to Calendar Versioning (https://calver.org/).
    * Project is now distributed as a `.tgz` or `.zip` with an executable `.jar`.
    * All code updated to read `resources` from inside `.jar` instead of directory.
* Moved from direct dependence on Log4J to SLF4J.
* Reorganized and renamed many files for clarity and consistency.
* Applied countless fixes based on "Static Analysis".
    * JDK migrations (boxing, un-boxing, etc.).
    * Implemented `try-with-resources` for all `java.lang.AutoCloseable` instances.
    * Removed calls to `printStackTrace()` or `System.out.println` in favor of proper logging.
* Reformatted code and cleaned up imports.
* Removed all calls to `assert`.
* Removed various forms of dead code and stale configurations.
* Removed calls to `commit()` during `Loader` operations.
* Refactored `Worker` and `Loader` usage of `Connection` objects and cleaned up transaction handling.
* Introduced [Dependabot](https://dependabot.com/) to keep Maven dependencies up to date.
* Simplified output flags by removing most of them, generally leaving the reporting functionality enabled by default.
* Provided an alternate `Catalog` that can be populated directly from the configured Benchmark database. The old catalog was proxied through `HSQLDB` -- this remains an option for DBMSes that may have incomplete catalog support.

## Citing This Repository

If you use this repository in an academic paper, please cite this repository:

> D. E. Difallah, A. Pavlo, C. Curino, and P. Cudré-Mauroux, "OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases," PVLDB, vol. 7, iss. 4, pp. 277-288, 2013.

The BibTeX is provided below for convenience.

```bibtex
@article{DifallahPCC13,
  author = {Djellel Eddine Difallah and Andrew Pavlo and Carlo Curino and Philippe Cudr{\'e}-Mauroux},
  title = {OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases},
  journal = {PVLDB},
  volume = {7},
  number = {4},
  year = {2013},
  pages = {277--288},
  url = {http://www.vldb.org/pvldb/vol7/p277-difallah.pdf},
}
```
