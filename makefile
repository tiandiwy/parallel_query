parallel_query:DBMysql.o parallel_query.o
	g++ DBMysql/DBMysql.o parallel_query.o -o parallel_query.so -lmysqlclient_r -L/usr/lib64/mysql -fPIC -Wall -O3 -shared -lpthread -pthread -lz -lm -lrt
DBMysql.o:DBMysql/DBMysql.cpp
	g++ -c DBMysql/DBMysql.cpp -o DBMysql/DBMysql.o -std=c++11 -fPIC -shared
parallel_query.o:parallel_query.cpp
	g++ -c parallel_query.cpp -o parallel_query.o -std=c++11 -fPIC -shared
