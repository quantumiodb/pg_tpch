DROP SCHEMA IF EXISTS tpch CASCADE;

CREATE SCHEMA tpch;

CREATE TABLE tpch.tpch_query_stats(
  ec_qid INT,
  ec_duration DOUBLE PRECISION,
  ec_recoed_time TIMESTAMP
);

create table tpch.tpch_tables(
  table_name varchar(100),
  status int, 
  child varchar(100),
  weight int);

--  customer |          1       
--  nation   |          1       
--  orders   |          10      
--  part     |          2       
--  region   |          1       
--  supplier |          1       


INSERT INTO tpch.tpch_tables(table_name, status, weight) VALUES ('customer', 0, 1);
INSERT INTO tpch.tpch_tables(table_name, status, weight) VALUES ('nation', 0, 1);
INSERT INTO tpch.tpch_tables(table_name, status, weight) VALUES ('region', 0, 1);
INSERT INTO tpch.tpch_tables(table_name, status, weight) VALUES ('supplier', 0, 1);

INSERT INTO tpch.tpch_tables(table_name, status, weight) VALUES ('lineitem', 2, 0);
INSERT INTO tpch.tpch_tables(table_name, status, weight) VALUES ('partsupp', 2, 0);

INSERT INTO tpch.tpch_tables(table_name, status, child, weight) VALUES ('orders', 1, 'lineitem', 10);
INSERT INTO tpch.tpch_tables(table_name, status, child, weight) VALUES ('part', 1, 'partsupp', 2);


CREATE TABLE tpch.tpch_host_info(host_core INT);
INSERT INTO tpch.tpch_host_info(host_core) VALUES (16);
update tpch.tpch_host_info set host_core = 20;
update tpch.tpch_host_info set host_core = 20;
