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

package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.SQLUtil;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/** TPC-C Benchmark Loader */
public final class TPCCLoader extends Loader<TPCCBenchmark> {

  private static final int FIRST_UNPROCESSED_O_ID = 2101;

  private final long numWarehouses;

  public TPCCLoader(TPCCBenchmark benchmark) {
    super(benchmark);
    numWarehouses = Math.max(Math.round(TPCCConfig.configWhseCount * this.scaleFactor), 1);
  }

  @Override
  public List<LoaderThread> createLoaderThreads() {
    final int numWh = (int) numWarehouses;

    // Shard count: available cores by default, -Dtpcc.load.shards=N to override.
    // Every unit is a deterministic (table, warehouse, chunk) slice with its own
    // RNG, so any N yields a valid dataset; the cap bounds the override.
    final int shards =
        Math.min(
            1024,
            Math.max(
                1,
                Integer.getInteger(
                    "tpcc.load.shards", Runtime.getRuntime().availableProcessors())));

    // Per-warehouse shard counts. STOCK splits by item range, CUSTOMER/HISTORY
    // and NEW_ORDER + ORDER_LINE by district range. The latter pair carries
    // about 3x the rows of STOCK, hence the 3x budget.
    final int stockShards = Math.max(1, (int) Math.ceil((double) shards / numWh));
    final int custShards = Math.min(TPCCConfig.configDistPerWhse, stockShards);
    final int orderShards =
        Math.min(TPCCConfig.configDistPerWhse, Math.max(1, (int) Math.ceil(3.0 * shards / numWh)));
    final int itemShards = Math.min(16, shards);

    // FK-parent latches: a child unit starts only after every shard of its
    // parent table(s) finished. Load order matters when the DDL runs against
    // an FK-enforcing engine such as InnoDB.
    final CountDownLatch itemLatch = new CountDownLatch(itemShards);
    final CountDownLatch[] whLatch = new CountDownLatch[numWh + 1];
    final CountDownLatch[] stockLatch = new CountDownLatch[numWh + 1];
    final CountDownLatch[] custLatch = new CountDownLatch[numWh + 1];
    final CountDownLatch[] oorderLatch = new CountDownLatch[numWh + 1];
    for (int w = 1; w <= numWh; w++) {
      whLatch[w] = new CountDownLatch(1);
      stockLatch[w] = new CountDownLatch(stockShards);
      custLatch[w] = new CountDownLatch(custShards);
      oorderLatch[w] = new CountDownLatch(1);
    }

    // Units are collected per table and enqueued table-major: every unit of a
    // parent table precedes every unit of its children. A FIFO loader pool
    // picks tasks in queue order, so whenever a unit blocks on a latch its
    // parents already occupy other pool slots (running or done); the load
    // cannot deadlock for any pool size >= 1. Table-major ordering also keeps
    // the pool busy: a warehouse-major queue fills the pool with children of
    // one warehouse that can only wait for the single parent unit ahead of
    // them. Row content derives from generators seeded by (table, warehouse,
    // chunk) or by the row key, so the enqueue order does not change it;
    // timestamp columns take the load-time wall clock.
    List<LoaderThread> threads = new ArrayList<>();
    List<LoaderThread> itemUnits = new ArrayList<>();
    List<LoaderThread> whUnits = new ArrayList<>();
    List<LoaderThread> stockUnits = new ArrayList<>();
    List<LoaderThread> custUnits = new ArrayList<>();
    List<LoaderThread> oorderUnits = new ArrayList<>();
    List<LoaderThread> orderLineUnits = new ArrayList<>();

    // ITEM shards (no parent).
    for (int s = 0; s < itemShards; s++) {
      final int itemStart = chunkStart(TPCCConfig.configItemCount, itemShards, s);
      final int itemEnd = chunkEnd(TPCCConfig.configItemCount, itemShards, s);
      final int chunk = s;
      itemUnits.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              loadItems(conn, itemStart, itemEnd, unitRng("item", 0, chunk));
            }

            @Override
            public void afterLoad() {
              itemLatch.countDown();
            }
          });
    }

    // WAREHOUSE + DISTRICT per warehouse (no parent).
    for (int w = 1; w <= numWh; w++) {
      final int w_id = w;
      whUnits.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              RandomGenerator rng = unitRng("warehouse", w_id, 0);
              loadWarehouse(conn, w_id, rng);
              loadDistricts(conn, w_id, rng);
            }

            @Override
            public void afterLoad() {
              whLatch[w_id].countDown();
            }
          });
    }

    for (int w = 1; w <= numWh; w++) {
      final int w_id = w;

      // STOCK by item range: after ITEM and WAREHOUSE.
      for (int s = 0; s < stockShards; s++) {
        final int itemStart = chunkStart(TPCCConfig.configItemCount, stockShards, s);
        final int itemEnd = chunkEnd(TPCCConfig.configItemCount, stockShards, s);
        final int chunk = s;
        stockUnits.add(
            new LoaderThread(this.benchmark) {
              @Override
              public void load(Connection conn) throws SQLException {
                loadStock(conn, w_id, itemStart, itemEnd, unitRng("stock", w_id, chunk));
              }

              @Override
              public void beforeLoad() {
                awaitLatch(itemLatch);
                awaitLatch(whLatch[w_id]);
              }

              @Override
              public void afterLoad() {
                stockLatch[w_id].countDown();
              }
            });
      }

      // CUSTOMER + HISTORY by district range: after WAREHOUSE (districts).
      for (int s = 0; s < custShards; s++) {
        final int dStart = chunkStart(TPCCConfig.configDistPerWhse, custShards, s);
        final int dEnd = chunkEnd(TPCCConfig.configDistPerWhse, custShards, s);
        final int chunk = s;
        custUnits.add(
            new LoaderThread(this.benchmark) {
              @Override
              public void load(Connection conn) throws SQLException {
                RandomGenerator rng = unitRng("customer", w_id, chunk);
                loadCustomers(conn, w_id, dStart, dEnd, rng);
                loadCustomerHistory(conn, w_id, dStart, dEnd, rng);
              }

              @Override
              public void beforeLoad() {
                awaitLatch(whLatch[w_id]);
              }

              @Override
              public void afterLoad() {
                custLatch[w_id].countDown();
              }
            });
      }

      // OORDER for the whole warehouse: after CUSTOMER (o_c_id). One writer by
      // design: where the DDL gives it a unique secondary index, the in-write
      // check makes concurrent same-warehouse inserts abort each other.
      oorderUnits.add(
          new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) throws SQLException {
              loadOpenOrders(
                  conn, w_id, 1, TPCCConfig.configDistPerWhse, unitRng("oorder", w_id, 0));
            }

            @Override
            public void beforeLoad() {
              awaitLatch(custLatch[w_id]);
            }

            @Override
            public void afterLoad() {
              oorderLatch[w_id].countDown();
            }
          });

      // NEW_ORDER + ORDER_LINE by district range: after OORDER (o_id) and
      // STOCK (ol_supply_w_id, ol_i_id).
      for (int s = 0; s < orderShards; s++) {
        final int dStart = chunkStart(TPCCConfig.configDistPerWhse, orderShards, s);
        final int dEnd = chunkEnd(TPCCConfig.configDistPerWhse, orderShards, s);
        final int chunk = s;
        orderLineUnits.add(
            new LoaderThread(this.benchmark) {
              @Override
              public void load(Connection conn) throws SQLException {
                loadNewOrders(conn, w_id, dStart, dEnd);
                loadOrderLines(conn, w_id, dStart, dEnd, unitRng("orderline", w_id, chunk));
              }

              @Override
              public void beforeLoad() {
                awaitLatch(oorderLatch[w_id]);
                awaitLatch(stockLatch[w_id]);
              }
            });
      }
    }

    // Parent tables first, so no child can be dequeued before every unit it
    // waits on is already in the pool or finished.
    threads.addAll(itemUnits);
    threads.addAll(whUnits);
    threads.addAll(stockUnits);
    threads.addAll(custUnits);
    threads.addAll(oorderUnits);
    threads.addAll(orderLineUnits);

    if (workConf.getLoaderThreads() < Math.min(shards, threads.size())) {
      LOG.warn(
          "tpcc.load.shards={} built {} loader units but loaderThreads={}; the load stays"
              + " correct but a bigger <loaderThreads> uses more cores.",
          shards,
          threads.size(),
          workConf.getLoaderThreads());
    }

    return threads;
  }

  /** 1-based inclusive start of chunk s out of shards over total. */
  private static int chunkStart(int total, int shards, int s) {
    return (int) (1 + (long) total * s / shards);
  }

  /** 1-based inclusive end of chunk s out of shards over total. */
  private static int chunkEnd(int total, int shards, int s) {
    return (int) ((long) total * (s + 1) / shards);
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
  }

  /**
   * Deterministic per-unit RNG over (randomSeed, table, warehouse, chunk), making every RNG-derived
   * value independent of pool scheduling and identical across runs for the same shard counts.
   */
  private RandomGenerator unitRng(String table, int w_id, int chunk) {
    long seed = workConf.getRandomSeed();
    seed = seed * 1_000_003L + table.hashCode();
    seed = seed * 1_000_003L + w_id;
    seed = seed * 1_000_003L + chunk;
    // Folding to 32 bits can collide two units onto one stream; harmless, as
    // row identity comes from loop indices and the RNG only fills payload.
    return new RandomGenerator((int) (seed ^ (seed >>> 32)));
  }

  /**
   * Buffers rows and writes them as JDBC batches, re-sending the identical rows on an OCC conflict.
   * Re-sending is only safe while a batch commits atomically, which needs rewriteBatchedStatements
   * and batchSize * row width under max_allowed_packet; a partially committed batch would hit
   * duplicate keys on retry.
   *
   * <p>1213 = ER_LOCK_DEADLOCK. 1180 = ER_ERROR_DURING_COMMIT wrapping "Got error 149".
   */
  private final class BatchWriter {
    private static final int MAX_RETRIES = 30;
    private static final int SPLIT_AFTER = 5;

    private final PreparedStatement stmt;
    private final String table;
    private final int w_id;
    private final List<Object[]> rows = new ArrayList<>();

    BatchWriter(PreparedStatement stmt, String table, int w_id) {
      this.stmt = stmt;
      this.table = table;
      this.w_id = w_id;
    }

    void add(Object... row) throws SQLException {
      rows.add(row);
      if (rows.size() >= workConf.getBatchSize()) {
        flush();
      }
    }

    void flush() throws SQLException {
      flushRows(rows);
      rows.clear();
    }

    private void flushRows(List<Object[]> batch) throws SQLException {
      if (batch.isEmpty()) {
        return;
      }
      for (int attempt = 0; ; attempt++) {
        try {
          for (Object[] row : batch) {
            for (int i = 0; i < row.length; i++) {
              if (row[i] == null) {
                stmt.setNull(i + 1, Types.NULL);
              } else {
                stmt.setObject(i + 1, row[i]);
              }
            }
            stmt.addBatch();
          }
          stmt.executeBatch();
          stmt.clearBatch();
          return;
        } catch (SQLException se) {
          try {
            stmt.clearBatch();
          } catch (SQLException cleanup) {
            // Cleanup must not replace the failure being classified
            se.addSuppressed(cleanup);
          }
          boolean retryable =
              (se.getErrorCode() == 1213)
                  || (se.getErrorCode() == 1180
                      && se.getMessage() != null
                      && se.getMessage().contains("Got error 149"));
          if (!retryable) {
            throw se;
          }
          if (attempt >= SPLIT_AFTER && batch.size() > 1) {
            // A batch that keeps losing validation is halved to shrink the
            // conflict window. A deterministic per-row failure still exhausts
            // MAX_RETRIES at size 1 and fails loud.
            LOG.warn(
                "load {} w_id={} splitting batch of {} after {} conflicts",
                table,
                w_id,
                batch.size(),
                attempt + 1);
            int mid = batch.size() / 2;
            flushRows(batch.subList(0, mid));
            flushRows(batch.subList(mid, batch.size()));
            return;
          }
          if (attempt >= MAX_RETRIES) {
            throw se;
          }
          LOG.warn("load {} w_id={} deadlock, retry {}/{}", table, w_id, attempt + 1, MAX_RETRIES);
          try {
            // Full-jitter exponential backoff: sibling units released by the
            // same latch would retry in lockstep under a deterministic backoff
            // and can keep re-colliding.
            long capMs = Math.min(1000L, 10L << Math.min(attempt, 10));
            Thread.sleep(ThreadLocalRandom.current().nextLong(capMs + 1));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw se;
          }
        }
      }
    }
  }

  private PreparedStatement getInsertStatement(Connection conn, String tableName)
      throws SQLException {
    Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
    String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
    return conn.prepareStatement(sql);
  }

  /** Round to the column scale: Tsurugi rejects DECIMAL binds that would lose precision. */
  private static BigDecimal dec(double value, int scale) {
    return BigDecimal.valueOf(value).setScale(scale, java.math.RoundingMode.HALF_UP);
  }

  protected void loadItems(Connection conn, int itemStart, int itemEnd, RandomGenerator rng)
      throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_ITEM, 0);
      for (int i = itemStart; i <= itemEnd; i++) {
        Item item = new Item();
        item.i_id = i;
        item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, rng), rng);
        item.i_price = TPCCUtil.randomNumber(100, 10000, rng) / 100.0;

        // i_data: 90% a random string of length [26 .. 50], 10% with
        // "ORIGINAL" crammed somewhere in the middle.
        int randPct = TPCCUtil.randomNumber(1, 100, rng);
        int len = TPCCUtil.randomNumber(26, 50, rng);
        if (randPct > 10) {
          item.i_data = TPCCUtil.randomStr(len, rng);
        } else {
          int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), rng);
          item.i_data =
              TPCCUtil.randomStr(startORIGINAL - 1, rng)
                  + "ORIGINAL"
                  + TPCCUtil.randomStr(len - startORIGINAL - 9, rng);
        }

        item.i_im_id = TPCCUtil.randomNumber(1, 10000, rng);

        writer.add(item.i_id, item.i_name, dec(item.i_price, 2), item.i_data, item.i_im_id);
      }
      writer.flush();
    }
  }

  protected void loadWarehouse(Connection conn, int w_id, RandomGenerator rng) throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE)) {
      Warehouse warehouse = new Warehouse();
      warehouse.w_id = w_id;
      warehouse.w_ytd = 300000;

      // random within [0.0000 .. 0.2000]
      warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, rng)) / 10000.0;
      warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, rng), rng);
      warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
      warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
      warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
      warehouse.w_state = TPCCUtil.randomStr(3, rng).toUpperCase();
      warehouse.w_zip = "123456789";

      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_WAREHOUSE, w_id);
      writer.add(
          warehouse.w_id,
          dec(warehouse.w_ytd, 2),
          dec(warehouse.w_tax, 4),
          warehouse.w_name,
          warehouse.w_street_1,
          warehouse.w_street_2,
          warehouse.w_city,
          warehouse.w_state,
          warehouse.w_zip);
      writer.flush();
    }
  }

  protected void loadStock(
      Connection conn, int w_id, int itemStart, int itemEnd, RandomGenerator rng)
      throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_STOCK, w_id);
      for (int i = itemStart; i <= itemEnd; i++) {
        Stock stock = new Stock();
        stock.s_i_id = i;
        stock.s_w_id = w_id;
        stock.s_quantity = TPCCUtil.randomNumber(10, 100, rng);
        stock.s_ytd = 0;
        stock.s_order_cnt = 0;
        stock.s_remote_cnt = 0;

        // s_data: 90% a random string of length [26 .. 50], 10% with
        // "ORIGINAL" crammed somewhere in the middle.
        int randPct = TPCCUtil.randomNumber(1, 100, rng);
        int len = TPCCUtil.randomNumber(26, 50, rng);
        if (randPct > 10) {
          stock.s_data = TPCCUtil.randomStr(len, rng);
        } else {
          int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), rng);
          stock.s_data =
              TPCCUtil.randomStr(startORIGINAL - 1, rng)
                  + "ORIGINAL"
                  + TPCCUtil.randomStr(len - startORIGINAL - 9, rng);
        }

        writer.add(
            stock.s_w_id,
            stock.s_i_id,
            stock.s_quantity,
            dec(stock.s_ytd, 2),
            stock.s_order_cnt,
            stock.s_remote_cnt,
            stock.s_data,
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng),
            TPCCUtil.randomStr(24, rng));
      }
      writer.flush();
    }
  }

  protected void loadDistricts(Connection conn, int w_id, RandomGenerator rng) throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_DISTRICT, w_id);
      for (int d = 1; d <= TPCCConfig.configDistPerWhse; d++) {
        District district = new District();
        district.d_id = d;
        district.d_w_id = w_id;
        district.d_ytd = 30000;

        // random within [0.0000 .. 0.2000]
        district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, rng)) / 10000.0);

        district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
        district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, rng), rng);
        district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
        district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
        district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
        district.d_state = TPCCUtil.randomStr(3, rng).toUpperCase();
        district.d_zip = "123456789";

        writer.add(
            district.d_w_id,
            district.d_id,
            dec(district.d_ytd, 2),
            dec(district.d_tax, 4),
            district.d_next_o_id,
            district.d_name,
            district.d_street_1,
            district.d_street_2,
            district.d_city,
            district.d_state,
            district.d_zip);
      }
      writer.flush();
    }
  }

  protected void loadCustomers(Connection conn, int w_id, int dStart, int dEnd, RandomGenerator rng)
      throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_CUSTOMER, w_id);
      for (int d = dStart; d <= dEnd; d++) {
        for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {
          Timestamp sysdate = new Timestamp(System.currentTimeMillis());

          Customer customer = new Customer();
          customer.c_id = c;
          customer.c_d_id = d;
          customer.c_w_id = w_id;

          // discount is random between [0.0000 ... 0.5000]
          customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, rng) / 10000.0);

          if (TPCCUtil.randomNumber(1, 100, rng) <= 10) {
            customer.c_credit = "BC"; // 10% Bad Credit
          } else {
            customer.c_credit = "GC"; // 90% Good Credit
          }
          if (c <= 1000) {
            customer.c_last = TPCCUtil.getLastName(c - 1);
          } else {
            customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(rng);
          }
          customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, rng), rng);
          customer.c_credit_lim = 50000;

          customer.c_balance = -10;
          customer.c_ytd_payment = 10;
          customer.c_payment_cnt = 1;
          customer.c_delivery_cnt = 0;

          customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
          customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
          customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng), rng);
          customer.c_state = TPCCUtil.randomStr(3, rng).toUpperCase();
          // TPC-C 4.3.2.7: 4 random digits + "11111"
          customer.c_zip = TPCCUtil.randomNStr(4, rng) + "11111";
          customer.c_phone = TPCCUtil.randomNStr(16, rng);
          customer.c_since = sysdate;
          customer.c_middle = "OE";
          customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, rng), rng);

          writer.add(
              customer.c_w_id,
              customer.c_d_id,
              customer.c_id,
              dec(customer.c_discount, 4),
              customer.c_credit,
              customer.c_last,
              customer.c_first,
              dec(customer.c_credit_lim, 2),
              dec(customer.c_balance, 2),
              dec(customer.c_ytd_payment, 2),
              customer.c_payment_cnt,
              customer.c_delivery_cnt,
              customer.c_street_1,
              customer.c_street_2,
              customer.c_city,
              customer.c_state,
              customer.c_zip,
              customer.c_phone,
              customer.c_since,
              customer.c_middle,
              customer.c_data);
        }
      }
      writer.flush();
    }
  }

  protected void loadCustomerHistory(
      Connection conn, int w_id, int dStart, int dEnd, RandomGenerator rng) throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_HISTORY, w_id);
      for (int d = dStart; d <= dEnd; d++) {
        for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {
          Timestamp sysdate = new Timestamp(System.currentTimeMillis());

          History history = new History();
          history.h_c_id = c;
          history.h_c_d_id = d;
          history.h_c_w_id = w_id;
          history.h_d_id = d;
          history.h_w_id = w_id;
          history.h_date = sysdate;
          history.h_amount = 10;
          history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, rng), rng);

          writer.add(
              history.h_c_id,
              history.h_c_d_id,
              history.h_c_w_id,
              history.h_d_id,
              history.h_w_id,
              history.h_date,
              dec(history.h_amount, 2),
              history.h_data);
        }
      }
      writer.flush();
    }
  }

  protected void loadOpenOrders(
      Connection conn, int w_id, int dStart, int dEnd, RandomGenerator rng) throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_OPENORDER, w_id);
      for (int d = dStart; d <= dEnd; d++) {
        // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
        int[] c_ids = new int[TPCCConfig.configCustPerDist];
        for (int i = 0; i < TPCCConfig.configCustPerDist; ++i) {
          c_ids[i] = i + 1;
        }
        // Collections.shuffle exists, but there is no
        // Arrays.shuffle
        for (int i = 0; i < c_ids.length - 1; ++i) {
          int remaining = c_ids.length - i - 1;
          int swapIndex = rng.nextInt(remaining) + i + 1;

          int temp = c_ids[swapIndex];
          c_ids[swapIndex] = c_ids[i];
          c_ids[i] = temp;
        }

        for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {

          Oorder oorder = new Oorder();
          oorder.o_id = c;
          oorder.o_w_id = w_id;
          oorder.o_d_id = d;
          oorder.o_c_id = c_ids[c - 1];
          // o_carrier_id is set *only* for orders with ids < 2101
          // [4.3.3.1]
          if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
            oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, rng);
          } else {
            oorder.o_carrier_id = null;
          }
          oorder.o_ol_cnt = getRandomCount(w_id, c, d);
          oorder.o_all_local = 1;
          oorder.o_entry_d = new Timestamp(System.currentTimeMillis());

          writer.add(
              oorder.o_w_id,
              oorder.o_d_id,
              oorder.o_id,
              oorder.o_c_id,
              oorder.o_carrier_id,
              oorder.o_ol_cnt,
              oorder.o_all_local,
              oorder.o_entry_d);
        }
      }
      writer.flush();
    }
  }

  private int getRandomCount(int w_id, int c, int d) {
    Customer customer = new Customer();
    customer.c_id = c;
    customer.c_d_id = d;
    customer.c_w_id = w_id;

    Random random = new Random(customer.hashCode());

    return TPCCUtil.randomNumber(5, 15, random);
  }

  protected void loadNewOrders(Connection conn, int w_id, int dStart, int dEnd)
      throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_NEWORDER, w_id);
      for (int d = dStart; d <= dEnd; d++) {
        for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {

          // 900 rows in the NEW-ORDER table corresponding to the last
          // 900 rows in the ORDER table for that district (i.e.,
          // with NO_O_ID between 2,101 and 3,000)
          if (c >= FIRST_UNPROCESSED_O_ID) {
            NewOrder new_order = new NewOrder();
            new_order.no_w_id = w_id;
            new_order.no_d_id = d;
            new_order.no_o_id = c;

            writer.add(new_order.no_w_id, new_order.no_d_id, new_order.no_o_id);
          }
        }
      }
      writer.flush();
    }
  }

  protected void loadOrderLines(
      Connection conn, int w_id, int dStart, int dEnd, RandomGenerator rng) throws SQLException {
    try (PreparedStatement stmt = getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE)) {
      BatchWriter writer = new BatchWriter(stmt, TPCCConstants.TABLENAME_ORDERLINE, w_id);
      for (int d = dStart; d <= dEnd; d++) {
        for (int c = 1; c <= TPCCConfig.configCustPerDist; c++) {

          int count = getRandomCount(w_id, c, d);

          for (int l = 1; l <= count; l++) {
            OrderLine order_line = new OrderLine();
            order_line.ol_w_id = w_id;
            order_line.ol_d_id = d;
            order_line.ol_o_id = c;
            order_line.ol_number = l; // ol_number
            order_line.ol_i_id = TPCCUtil.randomNumber(1, TPCCConfig.configItemCount, rng);
            if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
              order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
              order_line.ol_amount = 0;
            } else {
              order_line.ol_delivery_d = null;
              // random within [0.01 .. 9,999.99]
              order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, rng) / 100.0);
            }
            order_line.ol_supply_w_id = order_line.ol_w_id;
            order_line.ol_quantity = 5;
            order_line.ol_dist_info = TPCCUtil.randomStr(24, rng);

            writer.add(
                order_line.ol_w_id,
                order_line.ol_d_id,
                order_line.ol_o_id,
                order_line.ol_number,
                order_line.ol_i_id,
                order_line.ol_delivery_d,
                dec(order_line.ol_amount, 2),
                order_line.ol_supply_w_id,
                order_line.ol_quantity,
                order_line.ol_dist_info);
          }
        }
      }
      writer.flush();
    }
  }
}
