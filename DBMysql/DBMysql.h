#ifndef DB_MYSQL_H
#define DB_MYSQL_H

#ifdef _MSC_VER
  #include <winsock2.h>
  #include <mysql.h>
#else
  #include <mysql/mysql.h>
#endif

#include <string>
#include <map>
#include <vector>
#include <iostream>
using namespace std;
typedef map<string, string> strMap;

/*mysql操作类，封装了c语言相关的api，可实现基本的查询、插入、修改和删除动作*/
class DBMysql
{
  protected:
    MYSQL *mysql; //代表一个到数据库的连接
    void *handle;//动态链接库语句柄
    void *myd_real_connect;
  private:
    string host; //连接的服务器
    string user; //用户名
    string password; //连接密码
    unsigned int port; //连接端口
    string db; //操作的数据库的名称
    MYSQL_RES *result; //操作的结果
    string query; //sql语句
    unsigned long long num; //返回查询得到的结果数
    string error; //错误提示信息
    unsigned int error_no;//错误编码
    unsigned int debug; //是否显示调试信息
    strMap info ; //查询语句返回一条结果
    vector<strMap> arrInfo; //查询语句可能会返回多条结果
    vector<string> fields; //返回查询结果的列
    bool boConnected;

    string sConnectedRunSQL;

    void disPlayError();
    void init();
  public:
    DBMysql(const string &host, const string &user, const string &password, unsigned int port, const string &ConnectedRunSQL = "");   // 构造函数
    DBMysql(); //构造函数
    void SetConnect(const string &host, const string &user, const string &password, unsigned int port, const string &ConnectedRunSQL = "");  //确定连接参数
    unsigned int DBConnect();//连接数据库
    void DBDisconnect();//关闭连接
    unsigned int DBSelect(const string &db);  //连接一个数据库
    unsigned int DBQuery(const string &q); //查询数据库
    strMap GetInfo(); //返回查询得到的一条结果
    vector<strMap> GetArray(); //返回查询得到的结果
    string GetError(); //返回错误信息
    unsigned int GetErrorNo(); //返回错误编码
    vector<string> GetFields();//返回查询后的列值
    unsigned int InsertData(const string &table, strMap &data, bool AutoQuotes = true);  //向数据库中插入一条数据
    unsigned long long GetLastID(); //返回最后一个自动增量的值
    unsigned long long GetNum(); //返回一条sql语句影响的行数
    unsigned int UpdateData(const string &table, strMap &data, const string &condition, bool AutoQuotes = true);   //根据条件修改一条数据
    unsigned int DeleteData(const string &table, const string &condition);   //根据条件删除数据

    bool Connected()
    {
      return boConnected;
    }
    string getHost()
    {
      return host;
    }
    string getUser()
    {
      return user;
    }
    string getPassword()
    {
      return password;
    }
    unsigned int getPort()
    {
      return port;
    }
    string getConnectedRunSQL()
    {
      return sConnectedRunSQL;  //设置一旦连接成就执行的SQL语句 一般是 Set NAMES 'utf8' 这样的语句
    }
    void setConnectedRunSQL(const string &s)
    {
      sConnectedRunSQL = s;
    }

	void clearGetData();//清除获取到的数据

    string EscapeString(const string &Str);//安全转义过滤函数

    ~DBMysql();//析构函数
};
#endif
