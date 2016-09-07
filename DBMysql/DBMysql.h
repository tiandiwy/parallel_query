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

/*mysql�����࣬��װ��c������ص�api����ʵ�ֻ����Ĳ�ѯ�����롢�޸ĺ�ɾ������*/
class DBMysql
{
  protected:
    MYSQL *mysql; //����һ�������ݿ������
    void *handle;//��̬���ӿ�����
    void *myd_real_connect;
  private:
    string host; //���ӵķ�����
    string user; //�û���
    string password; //��������
    unsigned int port; //���Ӷ˿�
    string db; //���������ݿ������
    MYSQL_RES *result; //�����Ľ��
    string query; //sql���
    unsigned long long num; //���ز�ѯ�õ��Ľ����
    string error; //������ʾ��Ϣ
    unsigned int error_no;//�������
    unsigned int debug; //�Ƿ���ʾ������Ϣ
    strMap info ; //��ѯ��䷵��һ�����
    vector<strMap> arrInfo; //��ѯ�����ܻ᷵�ض������
    vector<string> fields; //���ز�ѯ�������
    bool boConnected;

    string sConnectedRunSQL;

    void disPlayError();
    void init();
  public:
    DBMysql(const string &host, const string &user, const string &password, unsigned int port, const string &ConnectedRunSQL = "");   // ���캯��
    DBMysql(); //���캯��
    void SetConnect(const string &host, const string &user, const string &password, unsigned int port, const string &ConnectedRunSQL = "");  //ȷ�����Ӳ���
    unsigned int DBConnect();//�������ݿ�
    void DBDisconnect();//�ر�����
    unsigned int DBSelect(const string &db);  //����һ�����ݿ�
    unsigned int DBQuery(const string &q); //��ѯ���ݿ�
    strMap GetInfo(); //���ز�ѯ�õ���һ�����
    vector<strMap> GetArray(); //���ز�ѯ�õ��Ľ��
    string GetError(); //���ش�����Ϣ
    unsigned int GetErrorNo(); //���ش������
    vector<string> GetFields();//���ز�ѯ�����ֵ
    unsigned int InsertData(const string &table, strMap &data, bool AutoQuotes = true);  //�����ݿ��в���һ������
    unsigned long long GetLastID(); //�������һ���Զ�������ֵ
    unsigned long long GetNum(); //����һ��sql���Ӱ�������
    unsigned int UpdateData(const string &table, strMap &data, const string &condition, bool AutoQuotes = true);   //���������޸�һ������
    unsigned int DeleteData(const string &table, const string &condition);   //��������ɾ������

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
      return sConnectedRunSQL;  //����һ�����ӳɾ�ִ�е�SQL��� һ���� Set NAMES 'utf8' ���������
    }
    void setConnectedRunSQL(const string &s)
    {
      sConnectedRunSQL = s;
    }

	void clearGetData();//�����ȡ��������

    string EscapeString(const string &Str);//��ȫת����˺���

    ~DBMysql();//��������
};
#endif
