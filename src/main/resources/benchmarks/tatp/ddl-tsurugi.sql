-- Tsurugi TATP DDL. Differences from ddl-generic.sql:
--  * lowercase table names, matching TATPConstants (Tsurugi identifiers are case-sensitive)
--  * TINYINT/SMALLINT -> INT (unsupported types); the loader/procedures bind with setInt
--  * no FOREIGN KEY; the sub_nbr UNIQUE constraint becomes a plain secondary index
DROP TABLE IF EXISTS call_forwarding;
DROP TABLE IF EXISTS special_facility;
DROP TABLE IF EXISTS access_info;
DROP TABLE IF EXISTS subscriber;

CREATE TABLE subscriber
(
    s_id         INT         NOT NULL PRIMARY KEY,
    sub_nbr      VARCHAR(15) NOT NULL,
    bit_1        INT,
    bit_2        INT,
    bit_3        INT,
    bit_4        INT,
    bit_5        INT,
    bit_6        INT,
    bit_7        INT,
    bit_8        INT,
    bit_9        INT,
    bit_10       INT,
    hex_1        INT,
    hex_2        INT,
    hex_3        INT,
    hex_4        INT,
    hex_5        INT,
    hex_6        INT,
    hex_7        INT,
    hex_8        INT,
    hex_9        INT,
    hex_10       INT,
    byte2_1      INT,
    byte2_2      INT,
    byte2_3      INT,
    byte2_4      INT,
    byte2_5      INT,
    byte2_6      INT,
    byte2_7      INT,
    byte2_8      INT,
    byte2_9      INT,
    byte2_10     INT,
    msc_location INT,
    vlr_location INT
);

CREATE TABLE access_info
(
    s_id    INT NOT NULL,
    ai_type INT NOT NULL,
    data1   INT,
    data2   INT,
    data3   VARCHAR(3),
    data4   VARCHAR(5),
    PRIMARY KEY (s_id, ai_type)
);

CREATE TABLE special_facility
(
    s_id        INT NOT NULL,
    sf_type     INT NOT NULL,
    is_active   INT NOT NULL,
    error_cntrl INT,
    data_a      INT,
    data_b      VARCHAR(5),
    PRIMARY KEY (s_id, sf_type)
);

CREATE TABLE call_forwarding
(
    s_id       INT NOT NULL,
    sf_type    INT NOT NULL,
    start_time INT NOT NULL,
    end_time   INT,
    numberx    VARCHAR(15),
    PRIMARY KEY (s_id, sf_type, start_time)
);

CREATE INDEX idx_sub_nbr ON subscriber (sub_nbr);
CREATE INDEX idx_cf ON call_forwarding (s_id);
