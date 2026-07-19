-- Tsurugi TPC-C DDL. Differences from ddl-mysql.sql:
--  * no FOREIGN KEY / UNIQUE / SET (unsupported); PKs kept
--  * no DEFAULT clauses (the loader supplies every value)
--  * uppercase column names: Tsurugi result-set labels are case-sensitive and the
--    procedure SQL selects uppercase; table names stay lowercase to match the loader
--  * indexes created after the tables (Tsurugi requires empty tables) but before load
-- Requires the server default sql.lowercase_regular_identifiers=false: with folding
-- enabled the uppercase column labels the procedures read would come back lowercased.

DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS oorder;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS warehouse;

CREATE TABLE warehouse (
    W_ID       INT            NOT NULL,
    W_YTD      DECIMAL(12,2)  NOT NULL,
    W_TAX      DECIMAL(4,4)   NOT NULL,
    W_NAME     VARCHAR(10)    NOT NULL,
    W_STREET_1 VARCHAR(20)    NOT NULL,
    W_STREET_2 VARCHAR(20)    NOT NULL,
    W_CITY     VARCHAR(20)    NOT NULL,
    W_STATE    CHAR(2)        NOT NULL,
    W_ZIP      CHAR(9)        NOT NULL,
    PRIMARY KEY (W_ID)
);

CREATE TABLE item (
    I_ID    INT           NOT NULL,
    I_NAME  VARCHAR(24)   NOT NULL,
    I_PRICE DECIMAL(5,2)  NOT NULL,
    I_DATA  VARCHAR(50)   NOT NULL,
    I_IM_ID INT           NOT NULL,
    PRIMARY KEY (I_ID)
);

CREATE TABLE stock (
    S_W_ID       INT           NOT NULL,
    S_I_ID       INT           NOT NULL,
    S_QUANTITY   INT           NOT NULL,
    S_YTD        DECIMAL(8,2)  NOT NULL,
    S_ORDER_CNT  INT           NOT NULL,
    S_REMOTE_CNT INT           NOT NULL,
    S_DATA       VARCHAR(50)   NOT NULL,
    S_DIST_01    CHAR(24)      NOT NULL,
    S_DIST_02    CHAR(24)      NOT NULL,
    S_DIST_03    CHAR(24)      NOT NULL,
    S_DIST_04    CHAR(24)      NOT NULL,
    S_DIST_05    CHAR(24)      NOT NULL,
    S_DIST_06    CHAR(24)      NOT NULL,
    S_DIST_07    CHAR(24)      NOT NULL,
    S_DIST_08    CHAR(24)      NOT NULL,
    S_DIST_09    CHAR(24)      NOT NULL,
    S_DIST_10    CHAR(24)      NOT NULL,
    PRIMARY KEY (S_W_ID, S_I_ID)
);

CREATE TABLE district (
    D_W_ID      INT            NOT NULL,
    D_ID        INT            NOT NULL,
    D_YTD       DECIMAL(12,2)  NOT NULL,
    D_TAX       DECIMAL(4,4)   NOT NULL,
    D_NEXT_O_ID INT            NOT NULL,
    D_NAME      VARCHAR(10)    NOT NULL,
    D_STREET_1  VARCHAR(20)    NOT NULL,
    D_STREET_2  VARCHAR(20)    NOT NULL,
    D_CITY      VARCHAR(20)    NOT NULL,
    D_STATE     CHAR(2)        NOT NULL,
    D_ZIP       CHAR(9)        NOT NULL,
    PRIMARY KEY (D_W_ID, D_ID)
);

CREATE TABLE customer (
    C_W_ID         INT            NOT NULL,
    C_D_ID         INT            NOT NULL,
    C_ID           INT            NOT NULL,
    C_DISCOUNT     DECIMAL(4,4)   NOT NULL,
    C_CREDIT       CHAR(2)        NOT NULL,
    C_LAST         VARCHAR(16)    NOT NULL,
    C_FIRST        VARCHAR(16)    NOT NULL,
    C_CREDIT_LIM   DECIMAL(12,2)  NOT NULL,
    C_BALANCE      DECIMAL(12,2)  NOT NULL,
    C_YTD_PAYMENT  DECIMAL(12,2)  NOT NULL,
    C_PAYMENT_CNT  INT            NOT NULL,
    C_DELIVERY_CNT INT            NOT NULL,
    C_STREET_1     VARCHAR(20)    NOT NULL,
    C_STREET_2     VARCHAR(20)    NOT NULL,
    C_CITY         VARCHAR(20)    NOT NULL,
    C_STATE        CHAR(2)        NOT NULL,
    C_ZIP          CHAR(9)        NOT NULL,
    C_PHONE        CHAR(16)       NOT NULL,
    C_SINCE        TIMESTAMP      NOT NULL,
    C_MIDDLE       CHAR(2)        NOT NULL,
    C_DATA         VARCHAR(500)   NOT NULL,
    PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
);

CREATE TABLE history (
    H_C_ID   INT           NOT NULL,
    H_C_D_ID INT           NOT NULL,
    H_C_W_ID INT           NOT NULL,
    H_D_ID   INT           NOT NULL,
    H_W_ID   INT           NOT NULL,
    H_DATE   TIMESTAMP     NOT NULL,
    H_AMOUNT DECIMAL(6,2)  NOT NULL,
    H_DATA   VARCHAR(24)   NOT NULL
);

CREATE TABLE oorder (
    O_W_ID       INT       NOT NULL,
    O_D_ID       INT       NOT NULL,
    O_ID         INT       NOT NULL,
    O_C_ID       INT       NOT NULL,
    O_CARRIER_ID INT       NULL,
    O_OL_CNT     INT       NOT NULL,
    O_ALL_LOCAL  INT       NOT NULL,
    O_ENTRY_D    TIMESTAMP NOT NULL,
    PRIMARY KEY (O_W_ID, O_D_ID, O_ID)
);

CREATE TABLE new_order (
    NO_W_ID INT NOT NULL,
    NO_D_ID INT NOT NULL,
    NO_O_ID INT NOT NULL,
    PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)
);

CREATE TABLE order_line (
    OL_W_ID        INT           NOT NULL,
    OL_D_ID        INT           NOT NULL,
    OL_O_ID        INT           NOT NULL,
    OL_NUMBER      INT           NOT NULL,
    OL_I_ID        INT           NOT NULL,
    OL_DELIVERY_D  TIMESTAMP     NULL,
    OL_AMOUNT      DECIMAL(6,2)  NOT NULL,
    OL_SUPPLY_W_ID INT           NOT NULL,
    OL_QUANTITY    INT           NOT NULL,
    OL_DIST_INFO   CHAR(24)      NOT NULL,
    PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
);

CREATE INDEX idx_customer_name ON customer (C_W_ID, C_D_ID, C_LAST, C_FIRST);
CREATE INDEX idx_oorder_customer ON oorder (O_W_ID, O_D_ID, O_C_ID, O_ID);
