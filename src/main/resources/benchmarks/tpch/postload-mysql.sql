/*
This script runs after TPCH table creation and data loading.
It improves overall load performance by approximately 30%.
This script is referenced by the <afterLoad> parameter in
mysql/sample_tpch_config.xml.

Indexes for each table are added in a single ALTER so the storage engine can
scan and decode the table once and build every index from that one pass.
*/
ALTER TABLE region ADD UNIQUE INDEX r_rk (r_regionkey ASC);
ALTER TABLE nation
  ADD UNIQUE INDEX n_nk (n_nationkey ASC),
  ADD INDEX n_rk (n_regionkey ASC);
ALTER TABLE part ADD UNIQUE INDEX p_pk (p_partkey ASC);
ALTER TABLE supplier
  ADD UNIQUE INDEX s_sk (s_suppkey ASC),
  ADD INDEX s_nk (s_nationkey ASC);
ALTER TABLE partsupp
  ADD INDEX ps_pk (ps_partkey ASC),
  ADD INDEX ps_sk (ps_suppkey ASC),
  ADD UNIQUE INDEX ps_pk_sk (ps_partkey ASC, ps_suppkey ASC),
  ADD UNIQUE INDEX ps_sk_pk (ps_suppkey ASC, ps_partkey ASC);
ALTER TABLE customer
  ADD UNIQUE INDEX c_ck (c_custkey ASC),
  ADD INDEX c_nk (c_nationkey ASC);
ALTER TABLE orders
  ADD UNIQUE INDEX o_ok (o_orderkey ASC),
  ADD INDEX o_ck (o_custkey ASC),
  ADD INDEX o_od (o_orderdate ASC);
ALTER TABLE lineitem
  ADD INDEX l_ok (l_orderkey ASC),
  ADD INDEX l_pk (l_partkey ASC),
  ADD INDEX l_sk (l_suppkey ASC),
  ADD INDEX l_sd (l_shipdate ASC),
  ADD INDEX l_cd (l_commitdate ASC),
  ADD INDEX l_rd (l_receiptdate ASC),
  ADD INDEX l_pk_sk (l_partkey ASC, l_suppkey ASC),
  ADD INDEX l_sk_pk (l_suppkey ASC, l_partkey ASC);
