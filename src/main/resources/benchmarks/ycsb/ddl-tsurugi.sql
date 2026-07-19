-- Tsurugi is case-sensitive: the table must match YCSBConstants.TABLE_NAME ("usertable")
-- and the columns must match the uppercase names in the procedure SQL.
DROP TABLE IF EXISTS usertable;
CREATE TABLE usertable (
    YCSB_KEY INT PRIMARY KEY,
    FIELD1   VARCHAR(100),
    FIELD2   VARCHAR(100),
    FIELD3   VARCHAR(100),
    FIELD4   VARCHAR(100),
    FIELD5   VARCHAR(100),
    FIELD6   VARCHAR(100),
    FIELD7   VARCHAR(100),
    FIELD8   VARCHAR(100),
    FIELD9   VARCHAR(100),
    FIELD10  VARCHAR(100)
);
