multi_query:DBMysql.o multi_query.o
	g++ DBMysql/DBMysql.o multi_query.o -o multi_query.so -lmysqlclient_r -L/usr/lib64/mysql -fPIC -Wall -O3 -shared -lpthread -pthread -lz -lm -lrt
DBMysql.o:DBMysql/DBMysql.cpp
	g++ -c DBMysql/DBMysql.cpp -o DBMysql/DBMysql.o -std=c++11 -fPIC -shared
multi_query.o:multi_query.cpp
	g++ -c multi_query.cpp -o multi_query.o -std=c++11 -fPIC -shared
