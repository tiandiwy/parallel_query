DROP FUNCTION IF EXISTS run_multi_query;
DROP FUNCTION IF EXISTS multi_query_version;
DROP FUNCTION IF EXISTS run_parallel_query;
DROP FUNCTION IF EXISTS build_create_table_sql;
DROP FUNCTION IF EXISTS get_main_from;
DROP FUNCTION IF EXISTS is_select_sql;
DROP FUNCTION IF EXISTS my_milliseconds;
DROP FUNCTION IF EXISTS my_microseconds;
DROP FUNCTION IF EXISTS my_nanoseconds;
DROP FUNCTION IF EXISTS parallel_query_pool_size;
DROP FUNCTION IF EXISTS parallel_query_version;
DROP FUNCTION IF EXISTS parallel_query_clear_pool;
DROP FUNCTION IF EXISTS run_spider_pq;

CREATE FUNCTION run_parallel_query RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION build_create_table_sql RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION get_main_from RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION is_select_sql RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION my_milliseconds RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION my_microseconds RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION my_nanoseconds RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION parallel_query_pool_size RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION parallel_query_version RETURNS string SONAME 'parallel_query.so';
CREATE FUNCTION parallel_query_clear_pool RETURNS int SONAME 'parallel_query.so';
CREATE FUNCTION run_spider_pq RETURNS string SONAME 'parallel_query.so';