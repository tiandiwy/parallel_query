#!/bin/bash
service mysql stop
rm -f ./parallel_query.so
rm -f ./DBMysql/DBMysql.o
make
rm -f /usr/lib64/mysql/plugin/parallel_query.so
cp parallel_query.so /usr/lib64/mysql/plugin/
service mysql start
