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
import java.sql.Connection;
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
   * Set @_ldb_plan = '<plan>' on the connection via a plain Statement so the value reliably
   * propagates to the next DML on the same connection.
   *
   * <p>Using PreparedStatement with parameter binding for SET @_ldb_plan = ? has been observed to
   * occasionally not propagate the user variable to the next statement's THD when combined with the
   * rewriteBatchedStatements=true JDBC option configured in bench/config/tpcc.xml. Bypassing the
   * prepared-stmt path with a literal statement avoids that issue.
   *
   * <p>Single quotes in the plan text are escaped by doubling them ('' is the SQL-standard escape
   * and works regardless of the NO_BACKSLASH_ESCAPES sql_mode). The plan grammar produced by the
   * appendPlan* helpers does not contain backslashes, so no backslash handling is required.
   */
  protected static void setLdbPlanSession(Connection conn, String plan) throws SQLException {
    String escaped = plan.replace("'", "''");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET @_ldb_plan = '" + escaped + "'");
    }
  }
}
