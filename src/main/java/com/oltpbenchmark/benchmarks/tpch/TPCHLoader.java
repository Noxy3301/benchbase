/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 *   TPC-H implementation
 *
 *   Ben Reilly (bd.reilly@gmail.com)
 *   Ippokratis Pandis (ipandis@us.ibm.com)
 *
 */

package com.oltpbenchmark.benchmarks.tpch;

import static com.oltpbenchmark.benchmarks.tpch.TPCHConstants.*;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpch.util.CustomerGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.NationGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.PartGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.PartSupplierGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.RegionGenerator;
import com.oltpbenchmark.benchmarks.tpch.util.SupplierGenerator;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class TPCHLoader extends Loader<TPCHBenchmark> {
  public TPCHLoader(TPCHBenchmark benchmark) {
    super(benchmark);
  }

  private enum CastTypes {
    LONG,
    DOUBLE,
    STRING,
    DATE
  }

  private static final CastTypes[] customerTypes = {
    CastTypes.LONG, // c_custkey
    CastTypes.STRING, // c_name
    CastTypes.STRING, // c_address
    CastTypes.LONG, // c_nationkey
    CastTypes.STRING, // c_phone
    CastTypes.DOUBLE, // c_acctbal
    CastTypes.STRING, // c_mktsegment
    CastTypes.STRING // c_comment
  };

  private static final CastTypes[] lineitemTypes = {
    CastTypes.LONG, // l_orderkey
    CastTypes.LONG, // l_partkey
    CastTypes.LONG, // l_suppkey
    CastTypes.LONG, // l_linenumber
    CastTypes.DOUBLE, // l_quantity
    CastTypes.DOUBLE, // l_extendedprice
    CastTypes.DOUBLE, // l_discount
    CastTypes.DOUBLE, // l_tax
    CastTypes.STRING, // l_returnflag
    CastTypes.STRING, // l_linestatus
    CastTypes.DATE, // l_shipdate
    CastTypes.DATE, // l_commitdate
    CastTypes.DATE, // l_receiptdate
    CastTypes.STRING, // l_shipinstruct
    CastTypes.STRING, // l_shipmode
    CastTypes.STRING // l_comment
  };

  private static final CastTypes[] nationTypes = {
    CastTypes.LONG, // n_nationkey
    CastTypes.STRING, // n_name
    CastTypes.LONG, // n_regionkey
    CastTypes.STRING // n_comment
  };

  private static final CastTypes[] ordersTypes = {
    CastTypes.LONG, // o_orderkey
    CastTypes.LONG, // o_LONG, custkey
    CastTypes.STRING, // o_orderstatus
    CastTypes.DOUBLE, // o_totalprice
    CastTypes.DATE, // o_orderdate
    CastTypes.STRING, // o_orderpriority
    CastTypes.STRING, // o_clerk
    CastTypes.LONG, // o_shippriority
    CastTypes.STRING // o_comment
  };

  private static final CastTypes[] partTypes = {
    CastTypes.LONG, // p_partkey
    CastTypes.STRING, // p_name
    CastTypes.STRING, // p_mfgr
    CastTypes.STRING, // p_brand
    CastTypes.STRING, // p_type
    CastTypes.LONG, // p_size
    CastTypes.STRING, // p_container
    CastTypes.DOUBLE, // p_retailprice
    CastTypes.STRING // p_comment
  };

  private static final CastTypes[] partsuppTypes = {
    CastTypes.LONG, // ps_partkey
    CastTypes.LONG, // ps_suppkey
    CastTypes.LONG, // ps_availqty
    CastTypes.DOUBLE, // ps_supplycost
    CastTypes.STRING // ps_comment
  };

  private static final CastTypes[] regionTypes = {
    CastTypes.LONG, // r_regionkey
    CastTypes.STRING, // r_name
    CastTypes.STRING // r_comment
  };

  private static final CastTypes[] supplierTypes = {
    CastTypes.LONG, // s_suppkey
    CastTypes.STRING, // s_name
    CastTypes.STRING, // s_address
    CastTypes.LONG, // s_nationkey
    CastTypes.STRING, // s_phone
    CastTypes.DOUBLE, // s_acctbal
    CastTypes.STRING, // s_comment
  };

  private PreparedStatement getInsertStatement(Connection conn, String tableName)
      throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
    String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
    return conn.prepareStatement(sql);
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    List<LoaderThread> threads = new ArrayList<>();

    // Per-large-table shard count. Default = available processors (the Java
    // equivalent of C++ std::thread::hardware_concurrency()), so the load
    // auto-fits the machine; override with -Dtpch.load.shards=N. Upstream
    // benchbase loads each big table with a single thread, leaving 15/16 cores
    // idle (load is single-thread/RPC-round-trip bound, not CPU bound — see
    // docs/phase13_load_speed_investigation.md). The TPC-H generators support
    // deterministic (part, partCount) chunked generation (each part seeks its
    // RNG streams to startIndex via advanceRows), so N shards produce the EXACT
    // same dataset as one — no gaps/duplicates.
    final int n =
        Math.max(
            1, Integer.getInteger("tpch.load.shards", Runtime.getRuntime().availableProcessors()));
    final double scaleFactor = this.workConf.getScaleFactor();

    // The framework runs these threads in a fixed pool of size loaderThreads
    // (config; itself defaults to availableProcessors()). Total threads here =
    // 3 tiny single-shard tables + 5 sharded tables * n. Threads are submitted
    // in topological (parent-before-child) order into a FIFO pool, so pool <
    // total does NOT deadlock — a child latch-waiter can never be dequeued
    // before the parent shard that releases its latch — but it DOES waste pool
    // slots blocking at FK barriers and lose parallelism. Warn so the user
    // raises <loaderThreads> to fill every core (we do not cap or fail: the load
    // stays correct, and on a machine with < 8 cores even n=1 trips the floor of
    // 3 + 5 threads, so capping/failing would needlessly break small hosts).
    final int totalThreads = 3 + 5 * n;
    final int pool = this.workConf.getLoaderThreads();
    if (pool < totalThreads) {
      LOG.warn(
          "tpch.load.shards={} fills every core only with loaderThreads >= {}, but loaderThreads={}."
              + " Load stays correct (FK order preserved) but loses parallelism at FK barriers;"
              + " raise <loaderThreads> in the workload config.",
          n,
          totalThreads,
          pool);
    }

    // FK-parent latches sized to the producing table's shard count, so a child
    // table starts only after ALL parent shards finish (the load-order DAG = the
    // "critical sections"). Tiny tables (region/nation/supplier, <=10K rows) stay
    // single-sharded. The latches are added to the thread list parent-before-
    // child; with the framework's FIFO pool this is correct regardless of pool
    // size (see the loaderThreads note above) — a child waiter never blocks a
    // parent that is still queued.
    final CountDownLatch regionLatch = new CountDownLatch(1);
    final CountDownLatch nationLatch = new CountDownLatch(1);
    final CountDownLatch supplierLatch = new CountDownLatch(1);
    final CountDownLatch partsLatch = new CountDownLatch(n);
    final CountDownLatch customerLatch = new CountDownLatch(n);
    final CountDownLatch ordersLatch = new CountDownLatch(n);
    final CountDownLatch partsSuppLatch = new CountDownLatch(n);
    // lineitem is the terminal table: no child waits on it. This latch is only
    // counted down (never awaited) so addTable's signature stays uniform.
    final CountDownLatch lineitemLatch = new CountDownLatch(n);
    final CountDownLatch[] none = new CountDownLatch[] {};

    addTable(
        threads,
        TABLENAME_REGION,
        regionTypes,
        1,
        (p, pc) -> new RegionGenerator(),
        none,
        regionLatch);
    addTable(
        threads,
        TABLENAME_PART,
        partTypes,
        n,
        (p, pc) -> new PartGenerator(scaleFactor, p, pc),
        none,
        partsLatch);
    addTable(
        threads,
        TABLENAME_NATION,
        nationTypes,
        1,
        (p, pc) -> new NationGenerator(),
        new CountDownLatch[] {regionLatch},
        nationLatch);
    addTable(
        threads,
        TABLENAME_SUPPLIER,
        supplierTypes,
        1,
        (p, pc) -> new SupplierGenerator(scaleFactor, p, pc),
        new CountDownLatch[] {nationLatch},
        supplierLatch);
    addTable(
        threads,
        TABLENAME_CUSTOMER,
        customerTypes,
        n,
        (p, pc) -> new CustomerGenerator(scaleFactor, p, pc),
        new CountDownLatch[] {nationLatch},
        customerLatch);
    addTable(
        threads,
        TABLENAME_ORDER,
        ordersTypes,
        n,
        (p, pc) -> new OrderGenerator(scaleFactor, p, pc),
        new CountDownLatch[] {customerLatch},
        ordersLatch);
    addTable(
        threads,
        TABLENAME_PARTSUPP,
        partsuppTypes,
        n,
        (p, pc) -> new PartSupplierGenerator(scaleFactor, p, pc),
        new CountDownLatch[] {partsLatch, supplierLatch},
        partsSuppLatch);
    addTable(
        threads,
        TABLENAME_LINEITEM,
        lineitemTypes,
        n,
        (p, pc) -> new LineItemGenerator(scaleFactor, p, pc),
        new CountDownLatch[] {ordersLatch, partsSuppLatch},
        lineitemLatch);

    return threads;
  }

  /**
   * Add {@code shards} parallel loader threads for one table. Each shard loads a disjoint (part,
   * partCount) chunk via the TPC-H generator (deterministic, gap/duplicate-free), awaits {@code
   * waitFor} FK-parent latches before loading, and counts down {@code done} after — preserving the
   * load-order DAG while filling otherwise-idle cores.
   */
  private void addTable(
      List<LoaderThread> threads,
      String tableName,
      CastTypes[] types,
      int shards,
      java.util.function.BiFunction<Integer, Integer, Iterable<List<Object>>> generator,
      CountDownLatch[] waitFor,
      CountDownLatch done) {
    for (int s = 0; s < shards; s++) {
      final int part = s + 1;
      final int partCount = shards;
      threads.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              try (PreparedStatement statement = getInsertStatement(conn, tableName)) {
                List<Iterable<List<Object>>> generators = new ArrayList<>();
                generators.add(generator.apply(part, partCount));

                genTable(conn, statement, generators, types, tableName);
              }
            }

            @Override
            public void beforeLoad() {
              try {
                for (CountDownLatch l : waitFor) {
                  l.await();
                }
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public void afterLoad() {
              done.countDown();
            }
          });
    }
  }

  private void genTable(
      Connection conn,
      PreparedStatement prepStmt,
      List<Iterable<List<Object>>> generators,
      CastTypes[] types,
      String tableName) {
    for (Iterable<List<Object>> generator : generators) {
      try {
        int recordsRead = 0;
        for (List<Object> elems : generator) {
          for (int idx = 0; idx < types.length; idx++) {
            final CastTypes type = types[idx];
            switch (type) {
              case DOUBLE:
                prepStmt.setDouble(idx + 1, (Double) elems.get(idx));
                break;
              case LONG:
                prepStmt.setLong(idx + 1, (Long) elems.get(idx));
                break;
              case STRING:
                prepStmt.setString(idx + 1, (String) elems.get(idx));
                break;
              case DATE:
                prepStmt.setDate(idx + 1, (Date) elems.get(idx));
                break;
              default:
                throw new RuntimeException("Unrecognized type for prepared statement");
            }
          }

          ++recordsRead;
          prepStmt.addBatch();
          if ((recordsRead % workConf.getBatchSize()) == 0) {

            LOG.debug("writing batch {} for table {}", recordsRead, tableName);

            prepStmt.executeBatch();
            prepStmt.clearBatch();
          }
        }

        prepStmt.executeBatch();
      } catch (Exception e) {
        // Do NOT swallow: a failed batch must abort this shard. Otherwise
        // afterLoad() still counts down the table's latch, releasing child
        // tables to load against a partially-populated parent — and because
        // Helios does not enforce FK constraints, that corruption is silent
        // (no error, wrong row counts). Rethrow so ThreadUtil aborts the load.
        LOG.error("loader failed for table {}: {}", tableName, e.getMessage(), e);
        throw new RuntimeException("loader failed for table " + tableName, e);
      }
    }
  }
}
