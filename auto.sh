#!/bin/bash
service mysql stop
make
rm -f /usr/lib64/mysql/plugin/multi_query.so
cp multi_query.so /usr/lib64/mysql/plugin/
service mysql start
