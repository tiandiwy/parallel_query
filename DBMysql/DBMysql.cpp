#include "stdafx.h"

#include "DBMysql.h"
#include <assert.h>
#include <mutex>

std::mutex g_mysql_mutex;

/*���캯�����趨���ӵķ��������û���������Ͷ˿�*/
DBMysql::DBMysql(const string &host, const string &user, const string &password, unsigned int port = 3306)
{
  std::lock_guard<std::mutex> lock(g_mysql_mutex);
  boConnected = false;
  mysql = mysql_init(NULL);
  num = 0;
  error = "";
  error_no = 0;
  query = "";
  result = NULL;
  SetConnect(host, user, password, port);
}
DBMysql::DBMysql()
{
  std::lock_guard<std::mutex> lock(g_mysql_mutex);
  boConnected = false;
  mysql = mysql_init(NULL);
  num = 0;
  error = "";
  error_no = 0;
  query = "";
  result = NULL;
  SetConnect("", "", "", 3306);
}
/*�趨���ӵķ��������û���������Ͷ˿�*/
void DBMysql::SetConnect(const string &host, const string &user, const string &password, unsigned int port)
{
  if (boConnected)
    {
      DBDisconnect();
    }
  DBMysql::host = host;
  DBMysql::user = user;
  DBMysql::password = password;
  DBMysql::port = port;
}
/*�������ݿ�*/
unsigned int DBMysql::DBConnect()
{
  //std::lock_guard<std::mutex> lock(g_mysql_mutex);
  MYSQL *con;
  if(mysql == NULL)
    {
      error = "��ʼ��mysql����";
      error_no = 1;
      boConnected = false;
      return 1;
    }
  if (boConnected)
    {
      DBDisconnect();
    }
  con = mysql_real_connect(mysql, host.c_str(), user.c_str(), password.c_str(), NULL, port, NULL, 0);
  if(con == NULL)
    {
      boConnected = false;
      error = mysql_error(mysql);
      error_no = mysql_errno(mysql);
	  mysql = mysql_init(NULL);
      return error_no;
    }
  else
    {
      boConnected = true;
    }
  return 0;
}

/*�ر�����*/
void DBMysql::DBDisconnect()
{
  mysql_close(mysql);
  boConnected = false;
}
/*ѡ��һ�����ݿ�*/
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

/*ִ��sql���*/
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


/*��ȡ��ѯ�õ���һ�����*/
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
/*��ȡ��ѯ�õ������н��*/
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
/*��ȡsql���ִ��Ӱ�������*/
unsigned long long DBMysql::GetNum()
{
  return num;
}
/*��ȡ������id��*/
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
/*�����ݿ��������*/
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
/*���������޸�����*/
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
/*��������ɾ������*/
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

/*��ȡ���صĴ�����Ϣ*/
string DBMysql::GetError()
{
  return error;
}

/*��ȡ���صĴ�����Ϣ*/
unsigned int DBMysql::GetErrorNo()
{
  return error_no;
}

/*���ز�ѯ�����ֵ*/
vector<string> DBMysql::GetFields()
{
  /*
   field = mysql_fetch_fields(result);
   Ȼ��ͨ��field[i].name�����ڴ��д��󣬲�֪��Ϊʲô��������mysql��bug
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

/*��ȫת����˺���*/
std::string DBMysql::EscapeString(const string &Str)
{
  int iStrLen = Str.length();
  char *tStr = new char[iStrLen * 2 + 1];
  mysql_real_escape_string(mysql, tStr, Str.c_str(), iStrLen);
  string retStr(tStr);
  delete[] tStr;
  return retStr;
}

/*��������*/
DBMysql::~DBMysql()
{
  if(result != NULL)
    {
      mysql_free_result(result);
    }
  fields.clear();
  error = "";
  error_no = 0;
  info.clear();
  db = "";
  arrInfo.clear();
  mysql_close(mysql);
}