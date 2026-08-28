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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.types.DatabaseType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public abstract class TPCCProcedure extends Procedure {

  public abstract void run(
      Connection conn,
      Random gen,
      int terminalWarehouseID,
      int numWarehouses,
      int terminalDistrictLowerID,
      int terminalDistrictUpperID,
      TPCCWorker w)
      throws SQLException;

  /**
   * Set the helios_tx_plan session variable on the connection via a plain Statement. The engine
   * consumes the value when the next transaction starts, so it has to be set per transaction.
   *
   * <p>Using PreparedStatement with parameter binding for SET SESSION helios_tx_plan = ? has been
   * observed to occasionally not propagate the value to the next statement's THD when combined
   * with the rewriteBatchedStatements=true JDBC option configured in bench/config/tpcc.xml.
   * Bypassing the prepared-stmt path with a literal statement avoids that issue.
   *
   * <p>Single quotes in the plan text are escaped by doubling them ('' is the SQL-standard escape
   * and works regardless of the NO_BACKSLASH_ESCAPES sql_mode). The plan grammar produced by the
   * appendPlan* helpers does not contain backslashes, so no backslash handling is required.
   */
  protected static void setPrefetchPlanSession(Connection conn, String plan) throws SQLException {
    String escaped = plan.replace("'", "''");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET SESSION helios_tx_plan = '" + escaped + "'");
    }
  }

  /**
   * Bind a monetary value into a DECIMAL(p,scale) column. Tsurugi rejects binds whose extra
   * floating-point digits would lose precision (SQL-02011), so round to the column scale there;
   * other databases keep the historical setDouble path.
   */
  protected void setDecimal(PreparedStatement stmt, int index, double value, int scale)
      throws SQLException {
    if (getDbType() == DatabaseType.TSURUGI) {
      stmt.setBigDecimal(index, BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP));
    } else {
      stmt.setDouble(index, value);
    }
  }
}
