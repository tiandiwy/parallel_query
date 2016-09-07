#include "stdafx.h"

#include "DBMysql.h"
#include <assert.h>
#include <mutex>
#include <dlfcn.h>

#define ABSPATH "/usr/lib64"

std::mutex g_mysql_mutex;

void DBMysql::init()
{
  //std::lock_guard<std::mutex> lock(g_mysql_mutex);
  mysql = NULL;
  boConnected = false;
  myd_real_connect = nullptr;
  
  num = 0;
  error = "";
  error_no = 0;
  query = "";
  result = NULL;
  
  handle = dlopen(ABSPATH "/libmysqlclient_r.so", RTLD_NOW | RTLD_DEEPBIND);
  if (!handle)
    {
      error_no = 1;
      error = "Failed to open:" ABSPATH "/libmysqlclient_r.so";
    }
    else{
          myd_real_connect=dlsym(handle, "mysql_real_connect");
          if (!myd_real_connect)
             {
               error_no = 2;
               error = "Failed to load symbol mysql_real_connect";
              }
        }
}

/*构造函数，设定连接的服务器，用户名，密码和端口*/
DBMysql::DBMysql(const string &host, const string &user, const string &password, unsigned int port = 3306, const string &ConnectedRunSQL)
{
  init();
  SetConnect(host, user, password, port, ConnectedRunSQL);
}

DBMysql::DBMysql()
{
  init();
  SetConnect("", "", "", 3306);
}
/*设定连接的服务器，用户名，密码和端口*/
void DBMysql::SetConnect(const string &host, const string &user, const string &password, unsigned int port, const string &ConnectedRunSQL)
{
  if (boConnected) { DBDisconnect(); }
  DBMysql::host = host;
  DBMysql::user = user;
  DBMysql::password = password;
  DBMysql::port = port;
  sConnectedRunSQL = ConnectedRunSQL;
}
/*连接数据库*/
unsigned int DBMysql::DBConnect()
{
  //std::lock_guard<std::mutex> lock(g_mysql_mutex);
  MYSQL *con;
  mysql = mysql_init(NULL);
  if(mysql == NULL)
    {
      error = "mysql info is null";
      error_no = 1;
      boConnected = false;
      return 1;
    }
  if (boConnected) { DBDisconnect();}
  con = ((MYSQL *(*)(MYSQL *,const char *,const char *,const char *,const char *,unsigned int,const char *,unsigned long))myd_real_connect)
  	    (mysql, host.c_str(), user.c_str(), password.c_str(), NULL, port, NULL, 0);
  //con = mysql_real_connect(mysql, host.c_str(), user.c_str(), password.c_str(), NULL, port, NULL, 0);
  if(con == NULL)
    {
      boConnected = false;
      error = mysql_error(mysql);
      error_no = mysql_errno(mysql);
      std::lock_guard<std::mutex> lock(g_mysql_mutex);
      mysql = mysql_init(NULL);
      return error_no;
    }
  else
    {
      boConnected = true;
      if (!sConnectedRunSQL.empty())
        {
          if (DBQuery(sConnectedRunSQL) != 0)
            {
              boConnected = false;
              error = mysql_error(mysql);
              error_no = mysql_errno(mysql);
              std::lock_guard<std::mutex> lock(g_mysql_mutex);
              mysql = mysql_init(NULL);
              return error_no;
            }
        }

    }
  return 0;
}

/*关闭连接*/
void DBMysql::DBDisconnect()
{
  mysql_close(mysql);
  boConnected = false;
}
/*选择一个数据库*/
unsigned int DBMysql::DBSelect(const string &database)
{
  unsigned int re;
  if( mysql == NULL)
    {
      return 1;
    }
  if (!boConnected)
    {
      re = DBConnect();
      if (re != 0)
        {
          return re;
        }
    }
  db = database;
  re = mysql_select_db(mysql, db.c_str());
  if(re != 0)
    {
      error += mysql_error(mysql);
      error_no = mysql_errno(mysql);
    }
  return re;
}

/*执行sql语句*/
unsigned int DBMysql::DBQuery(const string &q)
{
  unsigned int re;
  if( mysql == NULL)
    {
      return 1;
    }
  if (!boConnected)
    {
      re = DBConnect();
      if (re != 0)
        {
          return re;
        }
    }
  //assert(!q.empty());
  if (result != NULL)
    {
      mysql_free_result(result);
    }

  while (!mysql_next_result(mysql))
    {
      result = mysql_store_result(mysql);
      mysql_free_result(result);
    }

  query = q;
  re = mysql_query(mysql, query.c_str());
  if(re == 0)
    {
      result = mysql_store_result(mysql);
      num = mysql_affected_rows(mysql);
      info.clear();
      arrInfo.clear();
      fields.clear();
    }
  else
    {
      re = mysql_errno(mysql);
      error = mysql_error(mysql);
      error_no = re;
      //cout << error << endl;
    }
  return re;
}


/*获取查询得到的一条结果*/
strMap DBMysql::GetInfo()
{
  MYSQL_ROW row;
  unsigned int i;
  if(info.size() > 0)
    {
      return info;
    }
  if(result != NULL)
    {
      GetFields();
      row = mysql_fetch_row(result);
      if(row != NULL)
        {
          for(i = 0; i < fields.size(); i++)
            {
              info[fields[i]] = (char *)row[i];
            }
        }
      //mysql_free_result(result);
    }
  return info;
}
/*获取查询得到的所有结果*/
vector<strMap> DBMysql::GetArray()
{
  MYSQL_ROW row;
  unsigned int i;
  char *chTemp = NULL;
  strMap tmp;
  if (mysql != NULL)
    {
      if (arrInfo.size() > 0)
        {
          return arrInfo;
        }
      if (result != NULL)
        {
          GetFields();
          while (row = mysql_fetch_row(result))
            {
              if (row != NULL)
                {
                  for (i = 0; i < fields.size(); i++)
                    {
                      chTemp = (char *)row[i];
                      if (chTemp != NULL)
                        {
                          tmp[fields[i]] = (char *)row[i];
                        }
                      else
                        {
                          tmp[fields[i]] = "";
                        }
                    }
                  arrInfo.push_back(tmp);
                }
            }
        }
    }
  return arrInfo;
}
/*获取sql语句执行影响的行数*/
unsigned long long DBMysql::GetNum()
{
  return num;
}
/*获取插入后的id号*/
unsigned long long DBMysql::GetLastID()
{
  if (!boConnected)
    {
      if (DBConnect() != 0)
        {
          return -1;
        }
    }
  return mysql_insert_id(mysql);
}
/*向数据库插入数据*/
unsigned int DBMysql::InsertData(const string &table, strMap &data, bool AutoQuotes)
{
  strMap::const_iterator iter;
  string q1;
  int flag = 0;
  if (mysql == NULL)
    {
      return 1;
    }
  if (table.empty())
    {
      return 2;
    }
  //assert(data != NULL);
  for(iter = data.begin(); iter != data.end(); iter++)
    {
      if(flag == 0)
        {
          q1 = "insert into ";
          q1 += table;
          q1 += " set ";
          q1 += iter->first;
          q1 += "=";
          if (AutoQuotes)
            {
              q1 += "'";
            }
          q1 += EscapeString(iter->second);
          if (AutoQuotes)
            {
              q1 += "'";
            }
          flag++;
        }
      else
        {
          q1 += ",";
          q1 += iter->first;
          q1 += "=";
          if (AutoQuotes)
            {
              q1 += "'";
            }
          q1 += EscapeString(iter->second);
          if (AutoQuotes)
            {
              q1 += "'";
            }
        }
    }
  return DBQuery(q1);
}
/*根据条件修改数据*/
unsigned int DBMysql::UpdateData(const string &table, strMap &data, const string &condition, bool AutoQuotes)
{
  strMap::const_iterator iter;
  string q1;
  int flag = 0;
  if (mysql == NULL)
    {
      return 1;
    }
  if (table.empty())
    {
      return 2;
    }
  //assert(data != NULL);
  for(iter = data.begin(); iter != data.end(); iter++)
    {
      if(flag == 0)
        {
          q1 = "update ";
          q1 += table;
          q1 += " set ";
          q1 += iter->first;
          q1 += "=";
          if (AutoQuotes)
            {
              q1 += "'";
            }
          q1 += EscapeString(iter->second);
          if (AutoQuotes)
            {
              q1 += "'";
            }
          flag++;
        }
      else
        {
          q1 += ",";
          q1 += iter->first;
          q1 += "=";
          if (AutoQuotes)
            {
              q1 += "'";
            }
          q1 += EscapeString(iter->second);
          if (AutoQuotes)
            {
              q1 += "'";
            }
        }
    }
  if(!condition.empty())
    {
      q1 += " where ";
      q1 += condition;
    }
  return DBQuery(q1);
}
/*根据条件删除数据*/
unsigned int DBMysql::DeleteData(const string &table, const string &condition)
{
  string q;
  if (mysql == NULL)
    {
      return 1;
    }
  if (table.empty())
    {
      return 2;
    }
  q = "delete from ";
  q += table;
  if(!condition.empty())
    {
      q += " where ";
      q += condition;
    }
  return DBQuery(q);
}

/*获取返回的错误信息*/
string DBMysql::GetError()
{
  return error;
}

/*获取返回的错误信息*/
unsigned int DBMysql::GetErrorNo()
{
  return error_no;
}

/*返回查询后的列值*/
vector<string> DBMysql::GetFields()
{
  /*
   field = mysql_fetch_fields(result);
   然后通过field[i].name访问在此有错误，不知道为什么，可能是mysql的bug
  */

  MYSQL_FIELD *field;
  if ((mysql == NULL) || (!boConnected) || (fields.size() > 0))
    {
      return fields;
    }
  while(field = mysql_fetch_field(result))
    {
      fields.push_back(field->name);
    }
  return fields;
}

void DBMysql::clearGetData()//清除获取到的数据
{
  error = ""; //错误提示信息
  error_no = 0;//错误编码
  info.clear(); //查询语句返回一条结果
  arrInfo.clear(); //查询语句可能会返回多条结果
  fields.clear(); //返回查询结果的列
  if (result != NULL)
    {
      mysql_free_result(result);
	  result = NULL;
    }
  while (!mysql_next_result(mysql))
    {
      result = mysql_store_result(mysql);
      mysql_free_result(result);
    }
}


/*安全转义过滤函数*/
std::string DBMysql::EscapeString(const string &Str)
{
  int iStrLen = Str.length();
  char *tStr = new char[iStrLen * 2 + 1];
  mysql_real_escape_string(mysql, tStr, Str.c_str(), iStrLen);
  string retStr(tStr);
  delete[] tStr;
  return retStr;
}

/*析构函数*/
DBMysql::~DBMysql()
{
  if(result != NULL)
    {
      mysql_free_result(result);
    }
  while (!mysql_next_result(mysql))
    {
      result = mysql_store_result(mysql);
      mysql_free_result(result);
    }
  fields.clear();
  error = "";
  error_no = 0;
  info.clear();
  db = "";
  arrInfo.clear();
  mysql_close(mysql);
  myd_real_connect = nullptr;
  handle = nullptr;
}
