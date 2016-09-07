// lx.cpp : �������̨Ӧ�ó������ڵ㡣

#include "stdafx.h"
#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <regex>
#include <thread>
#include <mutex>
#include <chrono>

#ifdef _MSC_VER
  #define DBMysql_CONNECTED_SQL "Set NAMES 'gbk'"
  #include <winsock2.h>
  #include <mysql.h>
  #include <DBMysql.h>
  #include <DBMysql_Pool.hpp>
  #pragma comment(lib,"wsock32.lib")
  #pragma comment(lib,"libmysql.lib")
#else
  #define DBMysql_CONNECTED_SQL "Set NAMES 'utf8'"
  #include<sys/sysinfo.h>
  #include <mysql/mysql.h>
  #include "DBMysql/DBMysql.h"
  #include "DBMysql/DBMysql_Pool.hpp"
#endif

//��׼ͨ�ú��� ��ʼ
#define FG_QDF "___"
#define FG_HDF "____"
const std::string SPACE_CHARACTERS = " \t\r\n\x0B";
typedef std::map<std::string, std::map<std::string, int>> StrStrIntMap;

typedef std::vector<std::string> strlist;
typedef std::map<int, strMap> mapPRI_SQL_List;
typedef std::map<std::string, int> intMap;
const std::string MQ_COUNT_FIELD = "MQ_1MQ_COUNT_2FIELD3";

#define child_sql_table_postfix "__b"
#define CreateTableNamesFieldName "CreateTableNames"

struct SQLDBArray //һ����Ҫ�����±�Ľṹ
{
  std::string sql;
  vector<strMap> data;
  std::string errorstr;
  int errorno;
};
typedef std::map<int, SQLDBArray> SQLDBArrayList;

struct CreateTable //һ����Ҫ�����±�Ľṹ
{
  std::string sNewTableName;
  std::string sMainTable;
  std::string sCreateSQL;
  std::string sCreateSQL2;
  std::string sQuerySQL;
};
typedef std::map<std::string, CreateTable> ctMap;

int get_CPU_core_num(void)
{
#ifdef _MSC_VER
  SYSTEM_INFO info;
  GetSystemInfo(&info);
  return info.dwNumberOfProcessors;
#else
  return get_nprocs();   //GNU fuction
#endif
}

std::string GJM_EscapeSQL(const std::string &sSQL)
{
  std::string sRE;
  int iLen = sSQL.length();
  sRE.reserve(iLen * 2);
  char ch;
  bool boZY = false;
  for (int i = 0; i < iLen; i++)
    {
      ch = sSQL[i];
      switch (ch)
        {
          case '"':
            {
              if (!boZY)
                {
                  sRE += '\\';
                }
              else
                {
                  boZY = false;
                }
              break;
            }
          case '\\':
            {
              boZY = true;
              break;
            }
          default:
            {
              boZY = false;
              break;
            }
        }
      sRE += ch;
    }
  return std::move(sRE);
}

//in_str����һ��������ַ��� src��׼��Ҫ���滻��С�ַ��� dest��׼��Ҫ�滻Ϊ�Ľ���ַ���
//����ֵ���滻�����������ַ���
std::string StrReplace(std::string in_str, const std::string &src, const std::string &dest)
{
  int pos = in_str.find(src);//find��ʾ��in_str�ڲ���src_str
  if (pos != string::npos) //string::npos��ʾû�鵽
    {
      in_str = in_str.replace(pos, src.length(), dest);//����鵽�˾��ú���replace�滻
      //replace��һ�α�ʾҪ�滻��ʼλ�ã����α�ʾҪ�滻�ĳ��ȣ����α�ʾҪ�滻��ʲô
    }

  return std::move(in_str);
}

bool StrReplace2(std::string &in_str, const std::string &src, const std::string &dest)
{
  bool boRE = false;
  int pos = in_str.find(src);//find��ʾ��in_str�ڲ���src_str
  if (pos != string::npos) //string::npos��ʾû�鵽
    {
      boRE = true;
      in_str = in_str.replace(pos, src.length(), dest);//����鵽�˾��ú���replace�滻
      //replace��һ�α�ʾҪ�滻��ʼλ�ã����α�ʾҪ�滻�ĳ��ȣ����α�ʾҪ�滻��ʲô
    }

  return boRE;
}

bool getHeadString(const std::string &inputStr, char splitChar, std::string &sHead, std::string &sNR)
{
  bool boFind = false;
  std::string::size_type loc_start = inputStr.find_first_of(splitChar);

  if (loc_start != string::npos)
    {
      sHead = inputStr.substr(0, loc_start);
      sNR = inputStr.substr(loc_start + 1, inputStr.length());
      boFind = true;
    }
  else
    {
      sHead = inputStr;
      sNR = "";
      boFind = false;
    }
  return boFind;
}

//boNestingΪ0ʱ������ boNestingΪ1 ʱ����Ƕ���е����� (),",',` boNestingΪ2 ʱ����Ƕ���е����� (),",'
std::vector<std::string> GJM_split(const std::string &str, const std::string &sReg, bool icase = true, int iMaxCount = 0, int iNesting = 1)
{
  int iRCount = 0;
  std::string NewStr;
  std::string sNesting;
  std::vector<std::string> slRStr;

  std::string *pStr = (std::string *)&str;

  if (iNesting > 0)
    {
      int iLen = str.length();
      NewStr.reserve(iLen);
      sNesting.reserve(iLen);

      int iH = 0;
      char cType = 0;
      char ch = 0;
      for (int i = 0; i < iLen; i++)
        {
          ch = str[i];
          switch (ch)
            {
              case '(':
                {
                  if (iH <= 0)
                    {
                      cType = ch;
                      sNesting = "";
                    }
                  if (cType == ch)
                    {
                      iH++;
                      sNesting += ch;
                    }
                  break;
                }
              case ')':
                {
                  if ((iH > 0) && (cType == '('))
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          //slRStr.push_back(sNesting);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  break;
                }
              case '\'':
              case '"':
              case '`':
                {
                  if ((iNesting == 2) && (ch == '`'))
                    {
                      if (iH <= 0)
                        {
                          NewStr += ch;
                        }
                      else
                        {
                          sNesting += ch;
                        }
                      break;
                    }
                  if (iH <= 0)
                    {
                      cType = ch;
                      iH++;
                      sNesting = ch;
                    }
                  else if (cType == ch)
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          //slRStr.push_back(sNesting);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0)
                    {
                      NewStr += ch;
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
            }
        }
      pStr = &NewStr;
    }

  regex_constants::syntax_option_type r_type = icase ? regex_constants::icase : regex_constants::ECMAScript;
  std::regex reg(sReg, r_type);
  std::vector<std::string> vec;
  std::sregex_token_iterator it(pStr->begin(), pStr->end(), reg, -1);
  std::sregex_token_iterator end;
  int iCount = 0;
  int idx = 0;
  while (it != end)
    {
      if (iNesting > 0)
        {
          sNesting = *it++;
          for (; idx < iRCount; idx++)
            {
              if (!StrReplace2(sNesting, FG_QDF + to_string(idx) + FG_HDF, slRStr[idx]))
                {
                  break;
                }
            }
          vec.emplace_back(sNesting);
        }
      else
        {
          vec.emplace_back(*it++);
        }
      ++iCount;
      if ((iMaxCount > 0) && (iCount >= iMaxCount))
        {
          break;
        }
    }
  return std::move(vec);
}

int GJM_Regx(const std::string &sRegx, const std::string &Input, std::vector<std::string> &Result, bool icase = true, int iNesting = 1)
{
  std::smatch sResult;

  int iRCount = 0;
  std::string NewStr;
  std::string sNesting;
  std::vector<std::string> slRStr;

  std::string *pStr = (std::string *)&Input;

  if (iNesting > 0)
    {
      int iLen = Input.length();
      NewStr.reserve(iLen);
      sNesting.reserve(iLen);

      int iH = 0;
      char cType = 0;
      char ch = 0;
      for (int i = 0; i < iLen; i++)
        {
          ch = Input[i];
          switch (ch)
            {
              case '(':
                {
                  if (iH <= 0)
                    {
                      cType = ch;
                      sNesting = "";
                    }
                  if (cType == ch)
                    {
                      iH++;
                      sNesting += ch;
                    }
                  break;
                }
              case ')':
                {
                  if ((iH > 0) && (cType == '('))
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  break;
                }
              case '\'':
              case '"':
              case '`':
                {
                  if ((iNesting == 2) && (ch == '`'))
                    {
                      if (iH <= 0)
                        {
                          NewStr += ch;
                        }
                      else
                        {
                          sNesting += ch;
                        }
                      break;
                    }
                  if (iH <= 0)
                    {
                      cType = ch;
                      iH++;
                      sNesting = ch;
                    }
                  else if (cType == ch)
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0)
                    {
                      NewStr += ch;
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
            }
        }
      pStr = &NewStr;
    }

  regex_constants::syntax_option_type r_type = icase ? regex_constants::icase : regex_constants::ECMAScript;
  std::regex exp(sRegx, r_type);
  int iRE = 0;
  if (std::regex_search(*pStr, sResult, exp))
    {
      int iSize = sResult.size();
      for (int i = 0; i < iSize; ++i)
        {
          if (sResult[i].matched)
            {
              if (iNesting > 0)
                {
                  sNesting = sResult[i].str();
                  for (int idx = 0; idx < iRCount; idx++)
                    {
                      StrReplace2(sNesting, FG_QDF + to_string(idx) + FG_HDF, slRStr[idx]);
                    }
                  Result.emplace_back(sNesting);
                }
              else
                {
                  Result.emplace_back(sResult[i].str());
                }
              iRE++;
            }
        }
    }
  return iRE;
}

int GJM_Regx_All(const std::string &sRegx, const std::string &Input, std::vector<std::string> &Result, bool icase = true, int iNesting = 1)
{
  std::smatch sResult;

  int iRCount = 0;
  std::string NewStr;
  std::string sNesting;
  std::vector<std::string> slRStr;

  std::string *pStr = (std::string *)&Input;

  if (iNesting > 0)
    {
      int iLen = Input.length();
      NewStr.reserve(iLen);
      sNesting.reserve(iLen);

      int iH = 0;
      char cType = 0;
      char ch = 0;
      for (int i = 0; i < iLen; i++)
        {
          ch = Input[i];
          switch (ch)
            {
              case '(':
                {
                  if (iH <= 0)
                    {
                      cType = ch;
                      sNesting = "";
                    }
                  if (cType == ch)
                    {
                      iH++;
                      sNesting += ch;
                    }
                  break;
                }
              case ')':
                {
                  if ((iH > 0) && (cType == '('))
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  break;
                }
              case '\'':
              case '"':
              case '`':
                {
                  if ((iNesting == 2) && (ch == '`'))
                    {
                      if (iH <= 0)
                        {
                          NewStr += ch;
                        }
                      else
                        {
                          sNesting += ch;
                        }
                      break;
                    }
                  if (iH <= 0)
                    {
                      cType = ch;
                      iH++;
                      sNesting = ch;
                    }
                  else if (cType == ch)
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0)
                    {
                      NewStr += ch;
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
            }
        }
      pStr = &NewStr;
    }

  regex_constants::syntax_option_type r_type = icase ? regex_constants::icase : regex_constants::ECMAScript;
  std::regex exp(sRegx, r_type);

  int iRE = 0;
  const std::sregex_token_iterator end;  //��Ҫע��һ������
  for (std::sregex_token_iterator it(pStr->begin(), pStr->end(), exp); it != end; ++it)
    {
      if (iNesting > 0)
        {
          sNesting = it->str();
          for (int idx = 0; idx < iRCount; idx++)
            {
              StrReplace2(sNesting, FG_QDF + to_string(idx) + FG_HDF, slRStr[idx]);
            }
          Result.emplace_back(sNesting);
        }
      else
        {
          Result.emplace_back(it->str());
        }
      iRE++;
    }

  return iRE;
}

std::string GJM_RegxReplace(const std::string &sRegx, const std::string &replacer, const std::string &subject, bool icase = true, int iNesting = 1)
{
  int iRCount = 0;
  std::string NewStr;
  std::string sNesting;
  std::vector<std::string> slRStr;

  std::string *pStr = (std::string *)&subject;

  if (iNesting > 0)
    {
      int iLen = subject.length();
      NewStr.reserve(iLen);
      sNesting.reserve(iLen);

      int iH = 0;
      char cType = 0;
      char ch = 0;
      for (int i = 0; i < iLen; i++)
        {
          ch = subject[i];
          switch (ch)
            {
              case '(':
                {
                  if (iH <= 0)
                    {
                      cType = ch;
                      sNesting = "";
                    }
                  if (cType == ch)
                    {
                      iH++;
                      sNesting += ch;
                    }
                  break;
                }
              case ')':
                {
                  if ((iH > 0) && (cType == '('))
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  break;
                }
              case '\'':
              case '"':
              case '`':
                {
                  if ((iNesting == 2) && (ch == '`'))
                    {
                      if (iH <= 0)
                        {
                          NewStr += ch;
                        }
                      else
                        {
                          sNesting += ch;
                        }
                      break;
                    }
                  if (iH <= 0)
                    {
                      cType = ch;
                      iH++;
                      sNesting = ch;
                    }
                  else if (cType == ch)
                    {
                      iH--;
                      sNesting += ch;
                      if (iH <= 0)
                        {
                          NewStr += (FG_QDF + to_string(iRCount++) + FG_HDF);
                          slRStr.emplace_back(sNesting);
                        }
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0)
                    {
                      NewStr += ch;
                    }
                  else
                    {
                      sNesting += ch;
                    }
                  break;
                }
            }
        }
      pStr = &NewStr;
    }

  regex_constants::syntax_option_type r_type = icase ? regex_constants::icase : regex_constants::ECMAScript;
  std::regex exp(sRegx, r_type);
  sNesting = std::regex_replace(*pStr, exp, replacer);
  if (iNesting > 0)
    {
      for (int idx = 0; idx < iRCount; idx++)
        {
          StrReplace2(sNesting, FG_QDF + to_string(idx) + FG_HDF, slRStr[idx]);
        }
    }
  return std::move(sNesting);
}

bool CharInString(const char &ch, const char *pStr, int iLen)
{
  for (int i = 0; i < iLen; i++)
    {
      if (*pStr == ch)
        {
          return true;
        }
      else
        {
          ++pStr;
        }
    }
  return false;
}

std::string ltrim(const std::string &str, const std::string &character_mask = SPACE_CHARACTERS)
{
  std::string sRE;
  int iMaskLen = character_mask.length();
  int iStrLen = str.length();
  const char *pCM = character_mask.c_str();
  for (int i = 0; i < iStrLen; i++)
    {
      if (!CharInString(str[i], pCM, iMaskLen))
        {
          sRE = str.substr(i, iStrLen - i);
          break;
        }
    }
  return std::move(sRE);
}

std::string rtrim(const std::string &str, const std::string &character_mask = SPACE_CHARACTERS)
{
  std::string sRE;
  int iMaskLen = character_mask.length();
  int iStrLen = str.length();
  const char *pCM = character_mask.c_str();
  for (int i = iStrLen - 1; i >= 0; i--)
    {
      if (!CharInString(str[i], pCM, iMaskLen))
        {
          sRE = str.substr(0, i + 1);
          break;
        }
    }
  return std::move(sRE);
}

std::string trim(const std::string &str, const std::string &character_mask = SPACE_CHARACTERS)
{
  return std::move(rtrim(ltrim(str, character_mask), character_mask));
}

inline std::string strtolower(std::string str)
{
  transform(str.begin(), str.end(), str.begin(), ::tolower);
  return std::move(str);
}

inline std::string strtoupper(std::string str)
{
  transform(str.begin(), str.end(), str.begin(), ::toupper);
  return std::move(str);
}
//��׼ͨ�ú��� ����

class BuidlAllSQL_info
{
  public:
    bool boStatus;
    int iErrNo;
    std::string sErrStr;
    bool isSelect;
    //std::string sCreateSQL;
    std::string sTempTableName;
    std::vector<std::string> SQLs;
    std::string sEndSQL;
    //bool isDeleteTempTable;
  public:
    BuidlAllSQL_info(void)
    {
      Clear();
    }

    void Clear(void)
    {
      boStatus = false;
      iErrNo = 0;
      sErrStr = "";
      isSelect = false;
      //sCreateSQL = "";
      sTempTableName = "";
      SQLs.clear();
      sEndSQL = "";
      //isDeleteTempTable = false;
    }
};

class DecomposeSQL_info
{
  public:
    bool boStatus;
    bool isSelect;
    bool isUpdate;
    std::string sSQL_Select;
    std::string sSQL_From;
    std::string sSQL_Main_From;
    //std::string sSQL_Main_From_End;
    std::string sSQL_Where;
    std::string sSQL_End;
    std::string sSQL_End_All;
    std::string sSQL_Limit;
  public:
    DecomposeSQL_info(void)
    {
      Clear();
    }

    void Clear(void)
    {
      boStatus = false;
      isSelect = false; //$RE['is_select'] = (strtolower(substr(ltrim($sql, "\0\t\n\r\x0B ("), 0, 7)) == 'select ');
      isUpdate = false;
      sSQL_Select = "";
      sSQL_From = "";
      sSQL_Main_From = "";
      //sSQL_Main_From_End = "";
      sSQL_Where = "";
      sSQL_End = "";
      sSQL_End_All = "";
      sSQL_Limit = "";
    }
};

DBMysql_Pool<DBMysql> MySQLPool(50);

void ThreadRun(strlist *slAllSQL, int iIndex, const std::string &database, const std::string &sql, const std::string &host, const std::string &user, const std::string &password,
               unsigned int port = 3306)
{
  int iAllSQLCount = slAllSQL->size();
  if ((iAllSQLCount > iIndex) && (iIndex >= 0))//iIndex������ȷ
    {

      dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, host, user, password, port, DBMysql_CONNECTED_SQL, true);
      DBMysql *pMySQL = dsp.get();
      int iErrorNo = 0;
      if (!pMySQL->Connected())
        {
          iErrorNo = pMySQL->DBConnect();
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBSelect(trim(database));
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBQuery(sql);
        }
      if (0 == iErrorNo)
        {
          (*slAllSQL)[iIndex] = "+" + to_string(pMySQL->GetNum());
        }
      if (iErrorNo > 0)
        {
          iErrorNo = 0 - iErrorNo;
        }
      if (iErrorNo != 0)
        {
          (*slAllSQL)[iIndex] = " " + to_string(pMySQL->GetErrorNo()) + " " + pMySQL->GetError();
        }
      pMySQL->clearGetData();
    }
}

class mulit_query
{
  protected:
    std::string Orig_SQL;//ԭ��ѯSQL

    void ClearVar(void)//��ʼ����������
    {
      Orig_SQL = ""; //ԭ��ѯSQL
    }

  public://get set ����Ⱥ
    inline std::string getOrig_SQL(void)
    {
      return Orig_SQL;
    }
    inline void setOrig_SQL(const std::string &mOrig_SQL)
    {
      Orig_SQL = mOrig_SQL;
    }

  public://��������������
    mulit_query(void)
    {
      ClearVar();
    }

    ~mulit_query()
    {
      ClearVar();
    }

  public:
    //sMainForm ���û�б������������� ���� ��Ӧ��������ı���
    bool BuidlAllSQL(BuidlAllSQL_info &reInfo, const std::string &sDatabase, const std::string &sql, const std::string &sMainForm, const std::string &sPRIField, int iMinPRIValue,
                     int iMaxPRIValue, int MaxSQLCount,const std::string &TempName,const std::string &EndBefore = "", const std::string &EndSQL = "", bool boWhereToHeader = false)
    {
      reInfo.Clear();

      std::string tmpCount = "";
      map<std::string, std::string> strFieldMAP;

      Orig_SQL = sql;
      auto DeSQL = DecomposeSQL(sql); //�ȷֽ�SQL���
      if (DeSQL.boStatus)
        {
          reInfo.boStatus = true;
          reInfo.isSelect = DeSQL.isSelect;
        }
      else
        {
          reInfo.iErrNo = -2;
          reInfo.sErrStr = "~No from definition";
          reInfo.boStatus = false;
        }

      if (reInfo.boStatus)
        {
          if (DeSQL.isSelect)
            {
              if (EndSQL.empty())
                {
                  //strlist SelectArr = preg_split('/\bselect /i', $DeSQL['sql']['select']);
                  strlist SelectArr = GJM_split(DeSQL.sSQL_Select, "\\bselect\\s");
                  std::string sFields = "";
                  sFields.reserve(512);
                  if (SelectArr.size() > 1)
                    {
                      sFields = trim(SelectArr[1]);
                    }
                  if (sFields[sFields.length() - 1] != ',')
                    {
                      sFields += ','; //ĩβ���,������Ϊ���ó�����Զ֪�����һ���ֶν�����
                    }
                  int iFSLen = sFields.length();
                  std::string sFieldsOK = "";
                  sFieldsOK.reserve(256);
                  int iKH = 0; //��������
                  std::string sOneField = "";
                  sOneField.reserve(256);
                  std::string sFuncName = "";
                  sFuncName.reserve(256);
                  std::string sFuncParam = "";
                  sFuncParam.reserve(256);
                  std::string sAsName = "";
                  sAsName.reserve(256);
                  bool InAsName = false;
                  bool NeedZY = false;
                  bool isGruopFunc = false; //�Ƿ�Ϊ���ܺ���
                  char ch;
                  for (int i = 0; i < iFSLen; i++)
                    {
                      ch = sFields[i];
                      if (iKH > 0 && ((iKH == 1) && (ch != ')')))
                        {
                          sFuncParam += ch;
                        }
                      switch (sFields[i])
                        {
                          case ',': //��ʾ��ǰ�ֶν���
                            {
                              if (iKH > 0) //������������ڣ���ʾ�ֶλ�û�н��������','�Ǻ��������ָ���
                                {
                                  sOneField += ch;
                                }
                              else
                                {
                                  if (!InAsName)
                                    {
                                      if (sFuncName.length() <= 0)
                                        {
                                          if (NeedZY)
                                            {
                                              sFieldsOK += "`" + trim(sOneField) + "`,";
                                            }
                                          else
                                            {
                                              sFieldsOK += trim(sOneField) + ",";
                                            }
                                        }
                                      else
                                        {
                                          if (strtolower(sFuncName) == "avg")
                                            {
                                              sFuncName = "sum";
                                              if (tmpCount.empty())
                                                {
                                                  tmpCount = MQ_COUNT_FIELD;
                                                }
                                              strFieldMAP[sOneField] = sFuncName + "(`" + trim(sFuncParam) + "`) as `" + trim(sOneField) + "`";
                                              sFieldsOK += sFuncName + "(`" + trim(sOneField) + "`) / sum(" + tmpCount + ") as `" + trim(sOneField) + "`,";
                                            }
                                          else
                                            {
                                              sFieldsOK += sFuncName + "(`" + trim(sOneField) + "`) as `" + trim(sOneField) + "`,";
                                            }
                                        }
                                    }
                                  else
                                    {
                                      if ((sFuncName.length() <= 0) || (!isGruopFunc))
                                        {
                                          sFieldsOK += sAsName + ",";
                                        }
                                      else
                                        {
                                          //if ($isGruopFunc)
                                          //  {
                                          //    //����ǻ��ܺ�����Ҫ�������Ĳ����ǲ��Ǻ��в�����
                                          //    $FuncParam = rtrim(
                                          //                   $FuncParam);
                                          //    $FPLen = strlen($FuncParam) - 1;
                                          //    if ($FuncParam[$FPLen] == ')')
                                          //      {
                                          //        $FuncParam = substr($FuncParam, 0, $FPLen);
                                          //        $boHaveOperator = $this->HaveOperator($FuncParam);
                                          //      }
                                          //  }
                                          //else
                                          //  {
                                          //    //ֻҪ���ǻ��ܺ�����������Ҫ¶����ֶε�ԭ������
                                          //    $boHaveOperator = true;
                                          //  }
                                          //if ($boHaveOperator)
                                          //  {
                                          //    $FieldsOK . = $FuncName . '(`'.trim($OneField) . '`) as '.$AsName . ',';
                                          //  }
                                          //else
                                          //  {
                                          //    $FieldsOK . = $FuncName . '('.trim($AsName) . ') as '.$AsName . ',';
                                          //  }
                                          if (isGruopFunc)
                                            {
                                              if (strtolower(sFuncName) == "avg")
                                                {
                                                  sFuncName = "sum";
                                                  if (tmpCount.empty())
                                                    {
                                                      tmpCount = MQ_COUNT_FIELD;
                                                    }
                                                  strFieldMAP[sOneField] = sFuncName + "(`" + trim(sFuncParam) + "`)";
                                                  sFieldsOK += sFuncName + "(`" + trim(sAsName) + "`) / sum(" + tmpCount + ") as `" + trim(sAsName) + "`,";
                                                }
                                              else
                                                {
                                                  sFieldsOK += sFuncName + "(`" + trim(sAsName) + "`) as `" + trim(sAsName) + "`,";
                                                }
                                              //sFieldsOK += sFuncName + "(" + trim(sAsName) + ") as " + sAsName + ",";
                                            }
                                          else
                                            {
                                              sFieldsOK += sAsName + ",";
                                            }
                                        }
                                    }
                                  sOneField = "";
                                  sFuncName = "";
                                  sFuncParam = "";
                                  InAsName = false;
                                  sAsName = "";
                                  NeedZY = false;
                                  isGruopFunc = false;
                                }
                              break;
                            }
                          case '.':
                            {
                              if (!InAsName)
                                {
                                  if ((iKH > 0) || (StrEndIsNum(sOneField)))
                                    {
                                      sOneField += ch;
                                    }
                                  else
                                    {
                                      sOneField = "";
                                    }
                                }
                              else
                                {
                                  sAsName += ch;
                                }
                              break;
                            }
                          case '`':
                            {
                              if (!InAsName) //û�б����� ���Ҳ����ڻ��ܺ�����
                                {
                                  if ((!sFuncName.empty()) && isGruopFunc)
                                    {
                                      sOneField += "``";
                                      NeedZY = true;
                                    }
                                  else
                                    {
                                      sOneField += "`";
                                    }
                                }
                              else
                                {
                                  sAsName += "`";
                                }
                              break;
                            }
                          case '(':
                            {
                              ++iKH;
                              if (iKH == 1)
                                {
                                  sFuncName = trim(sOneField);
                                  std::string sFN = strtolower(sFuncName);

                                  bool boDefault = true;

                                  if (("avg" == sFN)
                                      || ("count" == sFN)
                                      || ("max" == sFN)
                                      || ("min" == sFN)
                                      || ("sum" == sFN)
                                      || ("bit_and" == sFN)
                                      || ("bit_or" == sFN)
                                      || ("bit_xor" == sFN)
                                      || ("group_concat" == sFN)
                                      || ("stddev_pop" == sFN)
                                      || ("stddev" == sFN)
                                      || ("std" == sFN)
                                      || ("stddev_samp" == sFN)
                                      || ("var_pop" == sFN)
                                      || ("variance" == sFN)
                                      || ("var_samp" == sFN))
                                    {
                                      if ("count" == sFN)
                                        {
                                          sFuncName = "sum";
                                        }
                                      isGruopFunc = true;
                                      boDefault = false;
                                    }
                                  if (boDefault)
                                    {
                                      isGruopFunc = false;
                                    }

                                  sFuncParam = "";
                                  NeedZY = true;
                                }
                              if (!InAsName)
                                {
                                  sOneField += ch;
                                }
                              else
                                {
                                  sAsName += ch;
                                }
                              break;
                            }
                          case ')':
                            {
                              if (iKH > 0)
                                {
                                  --iKH;
                                }
                              if (!InAsName)
                                {
                                  sOneField += ch;
                                }
                              else
                                {
                                  sAsName += ch;
                                }
                              break;
                            }
                          case ' ':
                          case '\n':
                          case '\t':
                            {
                              if (!InAsName)
                                {
                                  if (!sOneField.empty())
                                    {
                                      sOneField += ch;
                                      std::string sOF = rtrim(sOneField);
                                      int iOFLength = sOF.length();
                                      if (iOFLength >= 6)
                                        {
                                          std::string as_key = strtolower(sOF.substr(sOF.length() - 3, 3));
                                          if (((as_key == "\tas") || (as_key == "`as") || (as_key == " as") || (as_key == ")as")) && (iKH <= 0))
                                            {
                                              sOneField = rtrim(sOneField);
                                              sOneField = sOneField.substr(0, sOneField.length() - 2);
                                              sOneField = rtrim(sOneField);
                                              InAsName = true;
                                              sAsName = "";
                                            }
                                        }

                                    }
                                }
                              else
                                {
                                  sAsName += ch;
                                }
                              break;
                            }
                          default:
                            {
                              if (!InAsName)
                                {
                                  sOneField += ch;
                                }
                              else
                                {
                                  sAsName += ch;
                                }
                              break;
                            }
                        }
                    }
                  std::string SelectSQL = rtrim(sFieldsOK, ","); //������count����תΪsum����
                  std::string EndSQL = "`" + sDatabase + "`." + TempName + " " + (DeSQL.sSQL_End_All.empty() ? DeSQL.sSQL_End : DeSQL.sSQL_End_All)
                                       + (DeSQL.sSQL_Limit.empty() ? "" : " limit " + DeSQL.sSQL_Limit);
                  EndSQL = GJM_RegxReplace("\\b(?:\\s{0,}(?:`\\w{1,}`|\\w{1,})\\s{0,}\\.|\\s{0,}(?:`\\w{1,}`|\\w{1,})\\s{0,}\\.\\s{0,}(?:`\\w{1,}`|\\w{1,})\\s{0,}\\.)\\s{0,}(`\\w{1,}`|\\w{1,})\\s{0,1}(?=\\,{0,1})",
                                           " $1 ", EndSQL, true, 2);//�����б�������������ݿ��������ֶ�����ȥ�������������ݿ�����
                  reInfo.sEndSQL = EndBefore + " Select " + SelectSQL + (boWhereToHeader ? (tmpCount.empty() ? "" : "," + MQ_COUNT_FIELD) : "") + " from " + EndSQL;
                  if ((!tmpCount.empty()) && (!strFieldMAP.empty()))
                    {
                      for (auto &kv : strFieldMAP)
                        {
                          DeSQL.sSQL_Select = StrReplace(DeSQL.sSQL_Select, kv.first, kv.second);
                        }
                      DeSQL.sSQL_Select += ", count(*) as " + tmpCount;
                    }
                }
              else
                {
                  reInfo.sEndSQL = EndBefore + " " + EndSQL;
                }
            }

          if (MaxSQLCount <= 0)
            {
              MaxSQLCount = get_CPU_core_num();
            }
          int iMaxCount = iMaxPRIValue - iMinPRIValue + 1;
          int iMaxSQLCount = (iMaxCount > MaxSQLCount) ? MaxSQLCount : iMaxCount;
          int iAvgCount = iMaxCount / iMaxSQLCount;
          bool boFirst = true;
          int iBengin = 0, iEnd = 0;
          for (int i = 0; i < iMaxSQLCount; i++)
            {
              if (boFirst)
                {
                  boFirst = false;
                  iBengin = iMinPRIValue + i * iAvgCount;
                }
              else
                {
                  iBengin = iEnd + 1;
                }

              if (i != iMaxSQLCount - 1)
                {
                  iEnd = iBengin + iAvgCount;
                }
              else
                {
                  //ѭ�������һ��
                  iEnd = iMaxPRIValue;
                }
              std::string sWhereSQL = sMainForm + "." + sPRIField + " >= " + to_string(iBengin) + " and " + sMainForm + "." + sPRIField + " <= " + to_string(iEnd);
              std::string sInsertInto = DeSQL.isSelect ? ("INSERT INTO " + TempName + " ") : "";
              if (boWhereToHeader)
                {
                  reInfo.SQLs.emplace_back(to_string(iBengin) + "," + to_string(iEnd) + ";" + sInsertInto
                                           + DeSQL.sSQL_Select
                                           + (DeSQL.isUpdate ? " set " : " from ") + DeSQL.sSQL_From
                                           + (DeSQL.sSQL_Where.empty() ? "" : " where " + DeSQL.sSQL_Where)
                                           + " " + DeSQL.sSQL_End + (DeSQL.sSQL_Limit.empty() ? "" : " limit " + DeSQL.sSQL_Limit));
                }
              else
                {
                  reInfo.SQLs.emplace_back(sInsertInto + DeSQL.sSQL_Select + (DeSQL.isUpdate ? " set " : " from ") + DeSQL.sSQL_From + " where "
                                           + (DeSQL.sSQL_Where.empty() ? sWhereSQL : sWhereSQL + " and (" + DeSQL.sSQL_Where + ")")
                                           + " " + DeSQL.sSQL_End + (DeSQL.sSQL_Limit.empty() ? "" : " limit " + DeSQL.sSQL_Limit));
                }
            }
        }
      return reInfo.boStatus;
    }

    std::string Run(strlist &slAllSQL, const std::string &database, const std::string &host, const std::string &user, const std::string &password,
                    unsigned int port = 3306)
    {
      int iSQLCount = slAllSQL.size();
      int iMaxThread = MySQLPool.getmax();
      int iCount = 0;
      std::vector<std::thread> threads;
      for (int i = 0; i < iSQLCount; i++)
        {
          threads.emplace_back(std::bind(ThreadRun, &slAllSQL, i, database, slAllSQL[i], host, user, password, port));
          iCount++;
          //threads.push_back(std::thread(std::bind(ThreadRun, &slAllSQL, i, database, slAllSQL[i], host, user, password, port)));
          if (iCount >= iMaxThread)
            {
              for (auto &thread : threads)
                {
                  thread.join();
                }
              threads.clear();
              iCount = 0;
            }
        }
      if (iCount > 0)
        {
          for (auto &thread : threads)
            {
              thread.join();
            }
        }
      bool boAllOK = true;
      std::string sRE;
      sRE.reserve(256);
      for (auto &str : slAllSQL)
        {
          if (str.at(0) == ' ')
            {
              boAllOK = false;
            }
          sRE += str + ";";
        }
      return (boAllOK ? "" : sRE);
    }

  protected:

    bool StrEndIsNum(const std::string &sStr) //������һ����Ч�����Ƿ�Ϊ������
    {
      std::regex r("([^\\s]{1,}?)(?=\\b|$)");
      std::string EndStr;

      for (std::sregex_iterator it(sStr.begin(), sStr.end(), r), end; it != end; ++it)//end��β���������regex_iterator��regex_iterator��string���͵İ汾
        {
          EndStr = it->str();
        }
      std::regex r2("^[\\d]{1,}$");
      return std::regex_match(EndStr, r2);//return std::regex_search(EndStr, r2);
    }

    DecomposeSQL_info DecomposeSQL(std::string sql) //�ֽ�SQL���
    {
      DecomposeSQL_info RE;
      std::string sHead = strtolower(ltrim(sql, SPACE_CHARACTERS + "(").substr(0, 7));
      char ch7 = sHead[6];
      RE.isSelect = ((sHead.substr(0, 6) == "select")
                     && ((ch7 == ' ') || (ch7 == '\t') || (ch7 == '"') || (ch7 == '\'') || (ch7 == '`') || (ch7 == '(')));
      if (!RE.isSelect)
        {
          RE.isUpdate = ((sHead.substr(0, 6) == "update")
                         && ((ch7 == ' ') || (ch7 == '\t') || (ch7 == '"') || (ch7 == '\'') || (ch7 == '`') || (ch7 == '(')));
        }

      sql = trim(sql);
      strlist from_s;
      if (!RE.isUpdate)
        {
          from_s = GJM_split(sql, "\\bfrom\\b", true, 2); //preg_split('/\bfrom\b/i', $sql, 2);
        }
      else
        {
          from_s = GJM_split(sql, "\\bset\\b", true, 2); //preg_split('/\bfrom\b/i', $sql, 2);
        }
      if (from_s.size() == 2)
        {
          //��from �ؼ���
          RE.boStatus = true;
          if (RE.isUpdate)
            {
              strlist matches;
              int iCount = GJM_Regx("update.+?\\b(.+?)\\b", from_s[0], matches, true, 1);
              if (iCount > 1)
                {
                  RE.sSQL_Main_From = matches[1];
                  RE.sSQL_Select = from_s[0];
                  RE.sSQL_From = from_s[0].erase(0, 6);
                }
            }
          else
            {
              RE.sSQL_Select = from_s[0];
            }

          strlist where_s = GJM_split(from_s[1], "\\bwhere\\b", true, 2);//preg_split('/\bwhere\b/i', $from_s[1], 2);
          if (where_s.size() == 2)
            {
              //��where�ؼ���
              RE.sSQL_From = where_s[0];

              //strlist matches;
              //int iCount = GJM_Regx("(.*?)(?:\\border\\b|\\bwith\\b|\\bgroup\\b|\\blimit\\b)", where_s[1], matches, true, 1);
              strlist matches = GJM_split(where_s[1], "\\border\\b|\\bwith\\b|\\bgroup\\b|\\blimit\\b", true, 2, 1);

              if (matches.size() <= 0)
                {
                  //where ����û�йؼ�����
                  //$RE['sql']['where'] = $where_s[1];
                  RE.sSQL_Where = where_s[1];
                }
              else
                {
                  //$matches[1][0] ���� from ����
                  //$RE['sql']['where'] = $matches[1][0];
                  RE.sSQL_Where = matches[0];
                  //$RE['sql']['end'] = substr($where_s[1], strlen($matches[1][0])); //�Ϳ��Ի�ԭSQL
                  RE.sSQL_End = where_s[1].substr(matches[0].length(), where_s[0].length() - matches[0].length()); //�Ϳ��Ի�ԭSQL
                }
            }
          else
            {
              //û��where�ؼ���
              //preg_match_all('/(.*?)(?:\bleft\b|\bright\b|\binner\b|\bcross\b|\border\b|\bgroup\b|\blimit\b)/i',$from_s[1], $matches);
              //strlist matches;
              //int iCount = GJM_Regx("(.*?)(?:\\border\\b|\\bwith\\b|\\bgroup\\b|\\blimit\\b)", from_s[1], matches, true, 1);
              strlist matches = GJM_split(from_s[1], "\\border\\b|\\bwith\\b|\\bgroup\\b|\\blimit\\b", true, 2, 1);
              if (matches.size() <= 0)
                {
                  //û����������
                  //$RE['sql']['from'] = $from_s[1];
                  RE.sSQL_From = from_s[1];
                }
              else
                {
                  //$matches[1][0] ���� from ����
                  //$RE['sql']['from'] = $matches[1][0];
                  RE.sSQL_From = matches[0];
                  //$RE['sql']['end'] = substr($from_s[1], strlen($matches[1][0]));
                  RE.sSQL_End = from_s[1].substr(matches[0].length(), from_s[1].length() - matches[0].length());
                }
            }
        }
      else
        {
          //û��from����
          //$RE['ok'] = false;
          RE.boStatus = false;
        }
      if (RE.boStatus)
        {
          if (!RE.isUpdate)
            {
              std::string sFrom = trim(RE.sSQL_From, SPACE_CHARACTERS + "(");
              strlist FromArr = GJM_split(sFrom, "\\s|\\,|(?:as)|\\)", true, 2);

              //$RE['sql']['main_from']
              RE.sSQL_Main_From = trim(FromArr[0]);
              //$RE['sql']['main_from_end']
              //RE.sSQL_Main_From_End = sFrom.substr(FromArr[0].length(), sFrom.length() - FromArr[0].length());
            }
          if (RE.sSQL_Main_From.length() > 0)
            {
              //��ʼ���� limit
              //preg_match_all('/limit (\d{1,}\s{0,}\,{0,1}\s{0,}\d{0,}\s{0,})$/i', $RE['sql']['end'], $matches);
              strlist matches;
              int iCount = GJM_Regx("limit (\\d{1,}\\s{0,}\\,{0,1}\\s{0,}\\d{0,}\\s{0,})$", RE.sSQL_End, matches, true, 1);

              if (iCount > 0)
                {
                  //$RE['sql']['limit'] = $matches[1][0];
                  RE.sSQL_Limit = matches[1];
                  //$RE['sql']['end'] = substr($RE['sql']['end'], 0, 0 - strlen($matches[0][0]));
                  RE.sSQL_End = RE.sSQL_End.substr(0, RE.sSQL_End.length() - matches[0].length());
                }

              //$RE['sql']['end_all'] = $RE['sql']['end'];
              RE.sSQL_End_All = RE.sSQL_End;
              //��ʼ���� having
              //preg_match_all('/\bgroup\sby\b.*?\b(having.*?)(?=\border\b|\blimit\b|$)/i', $RE['sql']['end'], $matches);
              matches.clear();
              iCount = GJM_Regx("\\bgroup\\sby\\b.*?\\b(having.*?)(?=\\border\\b|\\bwith\\b|\\blimit\\b|$)", RE.sSQL_End, matches, true, 1);
              if (iCount > 0)
                {
                  //RE.sSQL_End = preg_replace('/(\bgroup\sby\b.*?)\bhaving\b.*?(?=\border\b|\blimit\b|$)/i', '$1', $RE['sql']['end']);
                  RE.sSQL_End = GJM_RegxReplace("(\\bgroup\\sby\\b.*?)\\bhaving\\b.*?(?=\\border\\b|\\bwith\\b|\\blimit\\b|$)", "$1", RE.sSQL_End, true, 1);
                }
              //��ʼ���� with
              matches.clear();
              iCount = GJM_Regx("\\bgroup\\sby\\b.*?\\b(with.*?)(?=\\border\\b|\\blimit\\b|$)", RE.sSQL_End, matches, true, 1);
              if (iCount > 0)
                {
                  //RE.sSQL_End = preg_replace('/(\bgroup\sby\b.*?)\bhaving\b.*?(?=\border\b|\blimit\b|$)/i', '$1', $RE['sql']['end']);
                  RE.sSQL_End = GJM_RegxReplace("(\\bgroup\\sby\\b.*?)\\bwith\\b.*?(?=\\border\\b|\\blimit\\b|$)", "$1", RE.sSQL_End, true, 1);
                }
            }
          else
            {
              //û��from����
              RE.boStatus = false;
            }
        }
      return std::move(RE);
    }
};

bool isSelectSQL(const std::string &sql)
{
  return (strtolower(ltrim(sql, SPACE_CHARACTERS + "(").substr(0, 7)) == "select ");
}

extern "C" long long is_select_sql(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  if (args->arg_count != 1)              //����������
    {
      strcpy(error, "is_select_sql() can onlyaccept one argument");
      return 0;
    }

  if (args->arg_type[0] != STRING_RESULT)    //����������
    {
      strcpy(error, "is_select_sql() argumenthas to be an string");
      return 0;
    }
  return isSelectSQL(args->args[0]) ? 1 : 0;//0�Ǵ��� 1����ȷ
}
extern "C" my_bool is_select_sql_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1)              //����������
    {
      strcpy(message, "is_select_sql() can onlyaccept one argument");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //����������
    {
      strcpy(message, "is_select_sql() argumenthas to be an string");
      return 1;
    }
  return 0;
}

bool getMainFrom(const std::string &sql, std::string &sMainName, std::string &sAsName)
{
  bool boRE = false;
  bool boQDF = false;//�Ƿ���ǰ���� `
  strlist from_s = GJM_split(sql, "\\bfrom\\b", true, 2);
  if (from_s.size() == 2)
    {
      from_s[1] = ltrim(from_s[1], SPACE_CHARACTERS + "(");
      if (from_s[1][0] == '`')
        {
          boQDF = true;
          from_s[1].erase(0, 1);
        }
      strlist FromArr = GJM_split(from_s[1], "`|\\s|\\,|\\)", true, 1);
      if (!FromArr.empty())
        {
          sMainName = std::move(FromArr[0]);
          int iMFLen = sMainName.length();
          if (boQDF)
            {
              sMainName = '`' + sMainName + '`';
            }
          std::string sAN = from_s[1].substr(iMFLen, from_s[1].length() - iMFLen);
          sAN = ltrim(sAN, SPACE_CHARACTERS + "`)");
          strlist Result;
          int iCount = GJM_Regx("^as\\s(.{1,})(?=\\s|\\b|$)", sAN, Result, 1);
          if (iCount >= 2)
            {
              sAsName = std::move(Result[1]);
            }
          else
            {
              sAsName = sMainName;
            }
          boRE = true;
        }
    }
  return boRE;
}

extern "C" char *get_main_from(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;
  *res_length = 0;
  if (args->arg_count != 1)              //����������
    {
      strcpy(error, "get_main_from() can onlyaccept one argument");
      *null_value = 1;
      return nullptr;
    }

  if (args->arg_type[0] != STRING_RESULT)    //����������
    {
      strcpy(error, "get_main_from() argumenthas to be an string");
      *null_value = 1;
      return nullptr;
    }
  std::string sMainName, sAsName;
  bool boOK = getMainFrom(args->args[0], sMainName, sAsName);
  if (!boOK)
    {
      strcpy(error, "There is no from string in the SQL statement");
      *null_value = 1;
      return nullptr;
    }
  std::string sRE;
  sRE.reserve(sMainName.length() + sAsName.length() + 1);
  sRE = sMainName + "," + sAsName;
  *res_length = sRE.length();

  initid->ptr = nullptr;
  if (*res_length > 255)
    {
      initid->ptr = new char[*res_length + 1];
      memcpy(initid->ptr, sRE.c_str(), *res_length);
      initid->ptr[*res_length] = '\0';
      return initid->ptr;
    }
  else
    {
      memcpy(result, sRE.c_str(), *res_length);
      result[*res_length] = '\0';
      return result;
    }
}
extern "C" my_bool get_main_from_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if (args->arg_count != 1)              //����������
    {
      strcpy(message, "get_main_from() can onlyaccept one argument");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //����������
    {
      strcpy(message, "get_main_from() argumenthas to be an string");
      return 1;
    }
  return 0;
}
extern "C" void get_main_from_deinit(UDF_INIT *initid)
{
  if (nullptr != initid->ptr)
    {
      delete[](initid->ptr);
      initid->ptr = nullptr;
    }
}

//string EngineType;// = "innodb"; //��Ĵ洢�������� create temporary table aa ENGINE=MEMORY (select * from recharge limit 0) ���п����ǡ�federated��
//string EngineConnection;// ��EngineTypeΪ"federated"���������Ҫ����Connection������
std::string BuildCreateTableSQL(std::string sql, const std::string &TempName, const std::string &EngineType = "innodb", const std::string &EngineConnection = "")
{
  const std::string sCreate = "CREATE TABLE " + TempName + " ENGINE=" + EngineType + " " + (EngineConnection.empty() ? "" : (" Connection='" + EngineConnection + "'"));
  sql = GJM_RegxReplace("(\\bselect\\b.*?avg\\b\\(.*?\\).*?)(?=from\\b)", "$1,count(*) as " + MQ_COUNT_FIELD + " ", sql, true, 0);

  strlist from_s = GJM_split(sql, "\\blimit\\b", true, 2);
  const std::string *pStr = &sql;
  if (from_s.size() == 2)
    {
      pStr = &(from_s[0]);
    }
  return std::move(sCreate + (*pStr) + " limit 0");
}

extern "C" char *build_create_table_sql(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;
  *res_length = 0;
  if ((args->arg_count != 2) && (args->arg_count != 4))              //����������
    {
      strcpy(error, "build_create_table_sql() 2 or 4 argumenthas");
      *null_value = 1;
      return nullptr;
    }
  for (unsigned int i = 0; i < args->arg_count; i++)
    {
      if (args->arg_type[i] != STRING_RESULT)    //����������
        {
          strcpy(error, "All parameters must be a string type");
          *null_value = 1;
          return nullptr;
        }
    }

  std::string sBuildSQL;
  if (2 == args->arg_count)
    {
      sBuildSQL = BuildCreateTableSQL(args->args[0], args->args[1]);
    }
  else
    {
      sBuildSQL = BuildCreateTableSQL(args->args[0], args->args[1], args->args[2], args->args[3]);
    }
  *res_length = sBuildSQL.length();
  initid->ptr = nullptr;
  if (*res_length > 255)
    {
      initid->ptr = new char[*res_length + 1];
      memcpy(initid->ptr, sBuildSQL.c_str(), *res_length);
      initid->ptr[*res_length] = '\0';
      return initid->ptr;
    }
  else
    {
      memcpy(result, sBuildSQL.c_str(), *res_length);
      result[*res_length] = '\0';
      return result;
    }
}
extern "C" my_bool build_create_table_sql_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if ((args->arg_count != 2) && (args->arg_count != 4))              //����������
    {
      strcpy(message, "build_create_table_sql() 2 or 4 argumenthas");
      return 1;
    }
  for (unsigned int i = 0; i < args->arg_count; i++)
    {
      if (args->arg_type[i] != STRING_RESULT)    //����������
        {
          strcpy(message, "All parameters must be a string type");
          return 1;
        }
    }
  return 0;
}
extern "C" void build_create_table_sql_deinit(UDF_INIT *initid)
{
  if (nullptr != initid->ptr)
    {
      delete[](initid->ptr);
      initid->ptr = nullptr;
    }
}

std::string RunMultiQuery(const std::string &sDatabase, const std::string &sql, const std::string &sMainForm,
                          const std::string &sPRIField, int iMinPRIValue, int iMaxPRIValue, int iMaxThreadCount,
                          const std::string &TempName, const std::string &EngineType,
                          const std::string &EndBefore, const std::string &EndSQL,
                          const std::string &host, const std::string &user, const std::string &password,
                          unsigned int port = 3306)
{
  bool boOK = true;
  std::string database = trim(sDatabase);
  mulit_query mq;
  BuidlAllSQL_info reInfo;
  if (isSelectSQL(sql))
    {
      //�Ƚ�����ʱ��
      dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, host, user, password, port, DBMysql_CONNECTED_SQL, true);
      DBMysql *pMySQL = dsp.get();
      int iErrorNo = 0;
      if (!pMySQL->Connected())
        {
          iErrorNo = pMySQL->DBConnect();
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBSelect(database);
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBQuery(BuildCreateTableSQL(sql, TempName, EngineType));
        }
      if (iErrorNo > 0)
        {
          iErrorNo = 0 - iErrorNo;
        }
      if (iErrorNo != 0)
        {
          reInfo.sErrStr = "~" + to_string(pMySQL->GetErrorNo()) + " " + pMySQL->GetError();
          boOK = false;
        }
      else
        {
          boOK = true;
        }
      pMySQL->clearGetData();
    }

  if (boOK)
    {
      boOK = mq.BuidlAllSQL(reInfo, database, sql, sMainForm, sPRIField, iMinPRIValue, iMaxPRIValue, iMaxThreadCount, TempName, EndBefore, EndSQL);
    }
  if (boOK)
    {
      std::string sRE = mq.Run(reInfo.SQLs, database, host, user, password, port);
      if (sRE.empty())
        {
          return std::move(reInfo.sEndSQL);
        }
      else
        {
          return "~" + sRE;
        }
    }
  else
    {
      return std::move(reInfo.sErrStr);
    }
}

extern "C" char *run_parallel_query(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;
  int port = 3306;
  if (args->arg_count == 15)              //����������
    {
      port = (int)* ((long long *)args->args[14]);
    }
  std::string  sRE = RunMultiQuery(args->args[0], args->args[1], args->args[2], args->args[3],
                                   (int)* ((long long *)args->args[4]), (int)* ((long long *)args->args[5]), (int)* ((long long *)args->args[6]),
                                   args->args[7], args->args[8], args->args[9], args->args[10], args->args[11], args->args[12], args->args[13], port);

  *res_length = sRE.length();
  initid->ptr = nullptr;
  if (*res_length > 255)
    {
      initid->ptr = new char[*res_length + 1];
      memcpy(initid->ptr, sRE.c_str(), *res_length);
      initid->ptr[*res_length] = '\0';
      return initid->ptr;
    }
  else
    {
      memcpy(result, sRE.c_str(), *res_length);
      result[*res_length] = '\0';
      return result;
    }
}
extern "C" my_bool run_parallel_query_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if ((args->arg_count != 14) && (args->arg_count != 15))              //����������
    {
      strcpy(message, "build_create_table_sql() 14 or 15 argumenthas");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //����������
    {
      strcpy(message, "Database parameter must be a string type");
      return 1;
    }
  if (args->arg_type[1] != STRING_RESULT)    //����������
    {
      strcpy(message, "SQL parameter must be a string type");
      return 1;
    }
  if (args->arg_type[2] != STRING_RESULT)    //����������
    {
      strcpy(message, "Main Form parameter must be a string type");
      return 1;
    }
  if (args->arg_type[3] != STRING_RESULT)    //����������
    {
      strcpy(message, "PRI Field parameter must be a string type");
      return 1;
    }
  if (args->arg_type[4] != INT_RESULT)    //����������
    {
      strcpy(message, "Min PRI value parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[5] != INT_RESULT)    //����������
    {
      strcpy(message, "Max PRI value parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[6] != INT_RESULT)    //����������
    {
      strcpy(message, "Max thread count parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[7] != STRING_RESULT)    //����������
    {
      strcpy(message, "Temp table name parameter must be a string type");
      return 1;
    }
  if (args->arg_type[8] != STRING_RESULT)    //����������
    {
      strcpy(message, "EngineType parameter must be a string type");
      return 1;
    }
  if (args->arg_type[9] != STRING_RESULT)    //����������
    {
      strcpy(message, "EndBefore SQL parameter must be a string type");
      return 1;
    }
  if (args->arg_type[10] != STRING_RESULT)    //����������
    {
      strcpy(message, "End SQL parameter must be a string type");
      return 1;
    }
  if (args->arg_type[11] != STRING_RESULT)    //����������
    {
      strcpy(message, "Host parameter must be a string type");
      return 1;
    }
  if (args->arg_type[12] != STRING_RESULT)    //����������
    {
      strcpy(message, "User parameter must be a string type");
      return 1;
    }
  if (args->arg_type[13] != STRING_RESULT)    //����������
    {
      strcpy(message, "Password parameter must be a string type");
      return 1;
    }
  if (args->arg_count == 15)
    {
      if (args->arg_type[14] != INT_RESULT)    //����������
        {
          strcpy(message, "Port parameter must be a integer type");
          return 1;
        }
    }
  return 0;
}
extern "C" void run_parallel_query_deinit(UDF_INIT *initid)
{
  if (nullptr != initid->ptr)
    {
      delete[](initid->ptr);
      initid->ptr = nullptr;
    }
}

//long long milliseconds_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now().time_since_epoch()).count();
long long myMilliseconds(void)
{
  return std::chrono::duration_cast<std::chrono::milliseconds> (std::chrono::system_clock::now().time_since_epoch()).count();
}

extern "C" long long my_milliseconds(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  return myMilliseconds();//0�Ǵ��� 1����ȷ
}
extern "C" my_bool my_milliseconds_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  return 0;
}

long long myMicroseconds(void)
{
  return std::chrono::duration_cast<std::chrono::microseconds> (std::chrono::system_clock::now().time_since_epoch()).count();
}

extern "C" long long my_microseconds(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  return myMicroseconds();//0�Ǵ��� 1����ȷ
}
extern "C" my_bool my_microseconds_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  return 0;
}

long long myNanoseconds(void)
{
  return std::chrono::duration_cast<std::chrono::nanoseconds> (std::chrono::system_clock::now().time_since_epoch()).count();
}

extern "C" long long my_nanoseconds(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  return myNanoseconds();//0�Ǵ��� 1����ȷ
}
extern "C" my_bool my_nanoseconds_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  return 0;
}

extern "C" long long parallel_query_pool_size(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  initid->ptr = nullptr;
  int iMax = 0;
  if (args->arg_count == 1)              //����������
    {
      if (args->arg_type[0] == INT_RESULT)    //����������
        {
          iMax = (int)* ((long long *)args->args[0]);
        }
    }
  if (iMax > 0)
    {
      MySQLPool.setmax(iMax);
    }
  return MySQLPool.getmax();
}

extern "C" my_bool parallel_query_pool_size_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if (args->arg_count > 1)              //����������
    {
      strcpy(message, "mysql_pool_size() 0 or 1 argumenthas");
      return 1;
    }
  if (args->arg_count == 1)              //����������
    {
      if (args->arg_type[0] != INT_RESULT)    //����������
        {
          strcpy(message, "pool size parameter must be a integer type");
          return 1;
        }
    }
  return 0;
}

extern "C" char *parallel_query_version(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;
  const std::string sVersion = "0.5.1.1";
  *res_length = sVersion.length();
  memcpy(result, sVersion.c_str(), *res_length);
  result[*res_length] = '\0';
  return result;

}
extern "C" my_bool parallel_query_version_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  return 0;
}

extern "C" long long parallel_query_clear_pool(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  initid->ptr = nullptr;
  MySQLPool.clear_idle();

  return 1;
}
extern "C" my_bool parallel_query_clear_pool_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  return 0;
}

void MainTable(mapPRI_SQL_List &newTableList, int idx, bool &boHaveSpiderTable, std::string sKey, const std::string &sDatabase, const std::string &sAllSpiderTables,
               std::string &sMainTable, bool &boMainTable, int &iMaxPRI, bool &boNewTable, bool boNeedLinkTable)
{
  if (!boHaveSpiderTable)
    {
      if (sAllSpiderTables.find("," + ((sKey.find('.') == std::string::npos/*δ�������ݿ�ǰ׺*/) ? sDatabase + "." : "") + sKey + ",") != std::string::npos)
        {
          boHaveSpiderTable = true;
          boMainTable = true;
        }
    }
  if ((!sKey.empty()) && (sKey[0] == '-'))
    {
      sKey.erase(sKey.begin());
    }
  if (boNeedLinkTable)
    {
      newTableList[idx][sKey] = sKey;
    }
  if (boMainTable)
    {
      sMainTable = sKey;
    }
  boMainTable = false;
  if (idx > iMaxPRI)
    {
      iMaxPRI = idx;
    }
  boNewTable = false;
}

//std::string &sNewTable �±���ǰ׺
int64_t i64TableCount = 0;
std::string fx_sql(mapPRI_SQL_List &newTableList, const std::string &sql, const std::string &sDatabase, const std::string &sAllSpiderTables, bool &boHaveSpiderTable,
                   std::string &sMainTable, std::string &sNewTable, int &iMaxPRI, int idx = 0, bool boNeedLinkTable = true, bool boOnlyTable = false)
{
  boHaveSpiderTable = false;
  std::string sRE;
  std::string sSend;

  sMainTable = "";
  int iLen = sql.length();
  sRE.reserve(iLen);
  sSend.reserve(iLen);
  int iKH = 0;
  char chTmp = '\0';
  char old_chTmp = '\0';
  bool boZY = false;//�Ƿ�ת��
  int iYH = 0;//���������� 0:�� 1:������ 2:˫���� 3:` 4:����`
  int iOld_YH = 0;//���������� 0:�� 1:������ 2:˫���� 3:` 4:����`
  int iFrom = 0; //0:��δ�ҵ� from. 1:����from �У�2:from ����
  std::string sKey;
  sKey.reserve(sql.length());
  std::string sTmpKey;
  std::string sYHKey;
  sYHKey.reserve(sql.length());
  bool boNewTable = false;
  bool boMainTable = true;
  bool boInDef;
  bool boAllZ_HaveSpiderTable = false;
  for (int i = 0; i < iLen; i++)
    {
      boInDef = false;
      iOld_YH = 0;
      old_chTmp = chTmp;
      chTmp = sql[i];
      switch (chTmp)
        {
          case '\r':
          case '\n':
            {
              chTmp = ' ';
              boInDef = true;
              break;
            }
          case '\\':
            {
              if (iKH <= 0)
                {
                  sRE += chTmp;
                }
              else
                {
                  sSend += chTmp;
                }
              if (i + 1 < iLen)
                {
                  i++;
                  if (iKH <= 0)
                    {
                      sRE += sql[i];
                    }
                  else
                    {
                      sSend += sql[i];
                    }
                }
              break;
            }
          case '(':
            {
              if (iYH == 0)
                {
                  if (iKH > 0)
                    {
                      sSend += chTmp;
                    }
                  ++iKH;
                  break;
                }
              boInDef = true;
              break;
            }
          case ')':
            {
              if (iYH == 0)
                {
                  --iKH;
                  if (iKH <= 0)
                    {
                      if ((strtolower(rtrim(ltrim(sSend).substr(0, 7))) == "select"))
                        {
                          bool boZ_HaveSpiderTable = false;
                          std::string sNewTableName = sNewTable;
                          std::string sNewMainTable;
                          std::string sNewSQL = fx_sql(newTableList, sSend, sDatabase, sAllSpiderTables, boZ_HaveSpiderTable, sNewMainTable, sNewTableName, iMaxPRI, idx + 1, boNeedLinkTable, boOnlyTable);
                          sNewSQL = sNewMainTable + ";" + sNewSQL;
                          if (!boOnlyTable)
                            {
                              if (boZ_HaveSpiderTable)
                                {
                                  if (newTableList[idx][sNewSQL].empty())
                                    {
                                      newTableList[idx][sNewSQL] = sNewTableName;
                                    }
                                  else
                                    {
                                      sNewTableName = newTableList[idx][sNewSQL];//����Ѿ��������ˣ���ֱ�������������
                                    }
                                }
                            }
                          if (boZ_HaveSpiderTable)
                            {
                              boAllZ_HaveSpiderTable = true;
                              sRE += sNewTableName + " ";
                            }
                          else
                            {
                              sRE += "(" + sSend + chTmp;
                            }
                        }
                      else
                        {
                          sRE += "(" + sSend + chTmp;
                        }
                      sSend.clear();
                    }
                  else
                    {
                      sSend += chTmp;
                    }
                  break;
                }
              boInDef = true;
              break;
            }
          case '\'':
            {
              if (iYH == 0)
                {
                  iYH = 1;
                  sYHKey.clear();
                }
              else if (iYH == 1)
                {
                  iOld_YH = 1;
                  iYH = 0;
                }
              boInDef = true;
              break;
            }
          case '"':
            {
              if (iYH == 0)
                {
                  iYH = 2;
                  sYHKey.clear();
                }
              else if (iYH == 2)
                {
                  iOld_YH = 2;
                  iYH = 0;
                }
              boInDef = true;
              break;
            }
          case '`':
            {
              if (iYH == 0)
                {
                  iYH = 3;//���뵥�� `״̬
                  sYHKey.clear();
                }
              else if (iYH == 3)
                {
                  if (old_chTmp == '`')
                    {
                      iYH = 4;//���� ����
                    }
                  else
                    {
                      iOld_YH = 3;
                      iYH = 0;
                      sYHKey += '`';
                    }
                }
              else if (iYH == 4)
                {
                  if (old_chTmp == '`')
                    {
                      iOld_YH = 4;
                      iYH = 0;
                      sYHKey += '`';
                    }
                }
              boInDef = true;
              break;
            }
          default:
            {
              boInDef = true;
              break;
            }
        }
      if (boInDef)
        {
          if (iKH <= 0)
            {
              sRE += chTmp;
              if (iYH <= 0)
                {
                  if (((chTmp >= 'a') && (chTmp <= 'z'))
                      || ((chTmp >= 'A') && (chTmp <= 'Z'))
                      || ((chTmp >= '0') && (chTmp <= '9'))
                      || (chTmp == '_')
                      || (chTmp == '.')
                      || ((chTmp == '-') && sKey.empty())
                     )
                    {
                      sKey += chTmp;
                    }
                  else
                    {
                      //��ʼ�����ؼ���
                      if (!sKey.empty())
                        {
                          switch (iFrom)
                            {
                              case 0:
                                {
                                  if (strtolower(sKey) == "from")
                                    {
                                      iFrom = 1;
                                      boNewTable = true;
                                    }
                                  break;
                                }
                              case 1:
                                {
                                  sTmpKey = strtolower(sKey);
                                  if ((sTmpKey == "where") || (sTmpKey == "order") || (sTmpKey == "with") || (sTmpKey == "group") || (sTmpKey == "limit"))
                                    {
                                      iFrom = 2;
                                      break;
                                    }
                                  if (sTmpKey == "as")
                                    {
                                      boNewTable = false;
                                    }
                                  if (boNewTable)
                                    {
                                      MainTable(newTableList, idx, boHaveSpiderTable, sKey, sDatabase, sAllSpiderTables, sMainTable, boMainTable, iMaxPRI, boNewTable, boNeedLinkTable);
                                    }
                                  else
                                    {
                                      if ((chTmp == ',') || (sTmpKey == "on"))
                                        {
                                          if (sTmpKey == "on")
                                            {
                                              boNewTable = false;
                                            }
                                          else if (chTmp == ',')
                                            {
                                              boNewTable = true;
                                            }
                                        }
                                      else if (sTmpKey == "join")
                                        {
                                          boNewTable = true;
                                        }
                                    }
                                  break;
                                }
                              default:
                                {
                                  break;
                                }
                            }
                          sKey.clear();
                        }
                      else
                        {
                          if (iFrom == 1)
                            {
                              switch (chTmp)
                                {
                                  case ',':
                                    {
                                      boNewTable = true;
                                      break;
                                    }
                                  case '`':
                                    {
                                      if ((iOld_YH >= 3) && boNewTable && (!sYHKey.empty()))
                                        {
                                          MainTable(newTableList, idx, boHaveSpiderTable, sYHKey, sDatabase, sAllSpiderTables, sMainTable, boMainTable, iMaxPRI, boNewTable, boNeedLinkTable);
                                        }
                                      break;
                                    }
                                }
                            }
                        }
                    }
                }
              else
                {
                  sYHKey += chTmp;
                }
            }
          else
            {
              sSend += chTmp;
            }
        }
    }
  if (boNewTable && (iFrom == 1) && (!sKey.empty()))
    {
      MainTable(newTableList, idx, boHaveSpiderTable, sKey, sDatabase, sAllSpiderTables, sMainTable, boMainTable, iMaxPRI, boNewTable, boNeedLinkTable);
    }
  sNewTable += "_" + to_string(idx) + "_" + to_string(myNanoseconds()) + "_" + to_string(i64TableCount++);
  return std::move(sRE);
}

std::string ThreadRunSQL(strlist &slAllSQL, const std::string &database, const std::string &host, const std::string &user, const std::string &password,
                         unsigned int port = 3306)
{
  int iSQLCount = slAllSQL.size();
  int iMaxThread = MySQLPool.getmax();
  int iCount = 0;
  std::vector<std::thread> threads;
  for (int i = 0; i < iSQLCount; i++)
    {
      threads.emplace_back(std::bind(ThreadRun, &slAllSQL, i, database, slAllSQL[i], host, user, password, port));
      iCount++;
      if (iCount >= iMaxThread)
        {
          for (auto &thread : threads)
            {
              thread.join();
            }
          threads.clear();
          iCount = 0;
        }
    }
  if (iCount > 0)
    {
      for (auto &thread : threads)
        {
          thread.join();
        }
    }
  bool boAllOK = true;
  std::string sRE;
  sRE.reserve(256);
  for (auto &str : slAllSQL)
    {
      if (str.at(0) == ' ')
        {
          boAllOK = false;
        }
      sRE += str + ";";
    }
  return (boAllOK ? "" : sRE);
}


void ThreadRunGetField(SQLDBArrayList *lSQLDBArray, int iIndex, const std::string &database, const std::string &host, const std::string &user, const std::string &password,
                       unsigned int port = 3306)
{
  SQLDBArrayList::iterator it = lSQLDBArray->find(iIndex);
  if (it != lSQLDBArray->end())
    {
      dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, host, user, password, port, DBMysql_CONNECTED_SQL, true);
      DBMysql *pMySQL = dsp.get();
      int iErrorNo = 0;
      if (!pMySQL->Connected())
        {
          iErrorNo = pMySQL->DBConnect();
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBSelect(trim(database));
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBQuery((*lSQLDBArray)[iIndex].sql);
        }
      if (0 == iErrorNo)
        {
          (*lSQLDBArray)[iIndex].data = pMySQL->GetArray();
          (*lSQLDBArray)[iIndex].errorno = 0;
          (*lSQLDBArray)[iIndex].errorstr = "";
        }
      else
        {
          (*lSQLDBArray)[iIndex].data.clear();
          (*lSQLDBArray)[iIndex].errorno = pMySQL->GetErrorNo();
          (*lSQLDBArray)[iIndex].errorstr = pMySQL->GetError();
        }
      pMySQL->clearGetData();
    }
}

void ThreadRunSQLGetField(SQLDBArrayList *lSQLDBArray, const std::string &database, const std::string &host, const std::string &user, const std::string &password,
                          unsigned int port = 3306)
{
  int iMaxThread = MySQLPool.getmax();
  int iCount = 0;
  std::vector<std::thread> threads;
  SQLDBArrayList::iterator cmtIter;
  SQLDBArrayList::iterator cmtIterEnd = (*lSQLDBArray).end();
  for (cmtIter = (*lSQLDBArray).begin(); cmtIter != cmtIterEnd; cmtIter++)
    {
      threads.emplace_back(std::bind(ThreadRunGetField, lSQLDBArray, cmtIter->first, database, host, user, password, port));
      iCount++;
      if (iCount >= iMaxThread)
        {
          for (auto &thread : threads)
            {
              thread.join();
            }
          threads.clear();
          iCount = 0;
        }
    }
  if (iCount > 0)
    {
      for (auto &thread : threads)
        {
          thread.join();
        }
    }
}

void ThreadMulitSQLRun(strlist *slAllRetrun, int iIndex, const strlist &sql_list, const std::string &database, const std::string &host, const std::string &user,
                       const std::string &password,
                       unsigned int port = 3306)
{
  int iAllSQLCount = slAllRetrun->size();
  if ((iAllSQLCount > iIndex) && (iIndex >= 0))//iIndex������ȷ
    {

      dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, host, user, password, port, DBMysql_CONNECTED_SQL, true);
      DBMysql *pMySQL = dsp.get();
      int iErrorNo = 0;
      if (!pMySQL->Connected())
        {
          iErrorNo = pMySQL->DBConnect();
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBSelect(trim(database));
        }
      if (0 == iErrorNo)
        {
          int iCount = sql_list.size();
          for (int i = 0; i < iCount; i++)
            {
              iErrorNo = pMySQL->DBQuery(sql_list[i]);
              if (iErrorNo != 0)
                {
                  break;
                }
            }
        }
      if (0 == iErrorNo)
        {
          //(*slAllRetrun)[iIndex] = "+" + to_string(pMySQL->GetNum());
        }
      if (iErrorNo > 0)
        {
          iErrorNo = 0 - iErrorNo;
        }
      if (iErrorNo != 0)
        {
          (*slAllRetrun)[iIndex] = " " + to_string(pMySQL->GetErrorNo()) + " " + pMySQL->GetError();
        }
      pMySQL->clearGetData();
    }
}

void ThreadMulitLinkTableSQLRun(strlist *slAllRetrun, int iIndex, const strlist &sql_list, vector<strMap> *ChildServers)
{
  const std::string sBegin = "CREATE TABLE `";
  int iAllSQLCount = slAllRetrun->size();
  if ((iAllSQLCount > iIndex) && (iIndex >= 0))//iIndex������ȷ
    {

      dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, (*ChildServers)[iIndex]["Host"], (*ChildServers)[iIndex]["Username"], (*ChildServers)[iIndex]["Password"],
                                         atoi((*ChildServers)[iIndex]["Port"].c_str()),
                                         DBMysql_CONNECTED_SQL, true);
      DBMysql *pMySQL = dsp.get();
      int iErrorNo = 0;
      if (!pMySQL->Connected())
        {
          iErrorNo = pMySQL->DBConnect();
        }
      if (0 == iErrorNo)
        {
          iErrorNo = pMySQL->DBSelect(trim((*ChildServers)[iIndex]["Db"]));
        }
      if (0 == iErrorNo)
        {
          int iCount = sql_list.size();
          int iBegin = 0, iEnd = 0;
          for (int i = 0; i < iCount; i++)
            {
              iErrorNo = pMySQL->DBQuery(sql_list[i]);
              if ((iErrorNo != 0) && (iErrorNo != 1050))
                {
                  break;
                }
              else
                {
                  if (iErrorNo == 0)
                    {
                      iBegin = sql_list[i].find(sBegin) + sBegin.length();
                      iEnd = sql_list[i].find("` (\r\n");
                      if (iEnd == string::npos)
                        {
                          iEnd = sql_list[i].find("` (\n");
                        }
                      (*ChildServers)[iIndex][CreateTableNamesFieldName] += sql_list[i].substr(iBegin, iEnd - iBegin) + ",";
                    }
                  iErrorNo = 0;
                }
            }
        }
      if (0 == iErrorNo)
        {
          (*slAllRetrun)[iIndex].clear();// = "+" + to_string(pMySQL->GetNum());
        }
      if (iErrorNo > 0)
        {
          iErrorNo = 0 - iErrorNo;
        }
      if (iErrorNo != 0)
        {
          (*slAllRetrun)[iIndex] = " " + to_string(pMySQL->GetErrorNo()) + " " + pMySQL->GetError();
        }
      pMySQL->clearGetData();
    }
}

void ThreadMulitDropTableSQLRun(const std::string &sServer_Name, std::map<std::string, strMap> &smAllChildCreateTables)
{
  dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, smAllChildCreateTables[sServer_Name]["Host"], smAllChildCreateTables[sServer_Name]["Username"],
                                     smAllChildCreateTables[sServer_Name]["Password"],
                                     atoi(smAllChildCreateTables[sServer_Name]["Port"].c_str()), DBMysql_CONNECTED_SQL, true);
  DBMysql *pMySQL = dsp.get();
  int iErrorNo = 0;
  if (!pMySQL->Connected())
    {
      iErrorNo = pMySQL->DBConnect();
    }
  if (0 == iErrorNo)
    {
      iErrorNo = pMySQL->DBSelect(trim(smAllChildCreateTables[sServer_Name]["Db"]));
    }
  if (0 == iErrorNo)
    {
      iErrorNo = pMySQL->DBQuery("drop table " + trim(smAllChildCreateTables[sServer_Name][CreateTableNamesFieldName], SPACE_CHARACTERS + ","));
    }
  pMySQL->clearGetData();
}

std::string ThreadRunMulitDropTableSQL(std::map<std::string, strMap> &smAllChildCreateTables)
{
  std::string sRE = "";
  int iMaxThread = MySQLPool.getmax();
  int iCount = 0;
  std::vector<std::thread> threads;
  try
    {
      std::map<std::string, strMap>::iterator Iter;
      std::map<std::string, strMap>::iterator IterEnd = smAllChildCreateTables.end();
      for (Iter = smAllChildCreateTables.begin(); Iter != IterEnd; Iter++)
        {
          //`Server_name`,`Host`,`Db`,`Username`,`Password`,`Port`,`Socket`,`Wrapper`,`Owner` from `mysql`.`servers`
          threads.emplace_back(std::bind(ThreadMulitDropTableSQLRun, Iter->first, smAllChildCreateTables));
          iCount++;
          if (iCount >= iMaxThread)
            {
              for (auto &thread : threads)
                {
                  thread.join();
                }
              threads.clear();
              iCount = 0;
            }
        }
      if (iCount > 0)
        {
          for (auto &thread : threads)
            {
              thread.join();
            }
        }
    }
  catch (exception &e)
    {
      sRE = e.what();
    }
  return std::move(sRE);
}

std::string ThreadRunMulitLinkTableSQL(strlist *slAllRetrun, const strlist &sql_list, vector<strMap> *ChildServers)
{
  std::string sRE = "";
  int iMaxThread = MySQLPool.getmax();
  int iCount = 0;
  std::vector<std::thread> threads;
  int iSize = ChildServers->size();
  try
    {
      for (int i = 0; i < iSize; i++)
        {
          //`Server_name`,`Host`,`Db`,`Username`,`Password`,`Port`,`Socket`,`Wrapper`,`Owner` from `mysql`.`servers`
          threads.emplace_back(std::bind(ThreadMulitLinkTableSQLRun, slAllRetrun, i, sql_list, ChildServers));
          iCount++;
          if (iCount >= iMaxThread)
            {
              for (auto &thread : threads)
                {
                  thread.join();
                }
              threads.clear();
              iCount = 0;
            }
        }
      if (iCount > 0)
        {
          for (auto &thread : threads)
            {
              thread.join();
            }
        }
    }
  catch (exception &e)
    {
      sRE = e.what();
    }
  return std::move(sRE);
}

std::string ThreadRunMulitSQL(strlist *slAllRetrun, const strlist &sql_list, vector<strMap> &ChildServers)
{
  std::string sRE = "";
  int iMaxThread = MySQLPool.getmax();
  int iCount = 0;
  std::vector<std::thread> threads;
  int iSize = ChildServers.size();
  try
    {
      for (int i = 0; i < iSize; i++)
        {
          //`Server_name`,`Host`,`Db`,`Username`,`Password`,`Port`,`Socket`,`Wrapper`,`Owner` from `mysql`.`servers`
          threads.emplace_back(std::bind(ThreadMulitSQLRun, slAllRetrun, i, sql_list, ChildServers[i]["Db"], ChildServers[i]["Host"], ChildServers[i]["Username"],
                                         ChildServers[i]["Password"], atoi(ChildServers[i]["Port"].c_str())));
          iCount++;
          if (iCount >= iMaxThread)
            {
              for (auto &thread : threads)
                {
                  thread.join();
                }
              threads.clear();
              iCount = 0;
            }
        }
      if (iCount > 0)
        {
          for (auto &thread : threads)
            {
              thread.join();
            }
        }
    }
  catch (exception &e)
    {
      sRE = e.what();
    }
  return std::move(sRE);
}
//const std::string &sHost, const std::string &sUsername, const std::string &sPassword, int iPort, const std::string &sDatabase MYSQL���������Ϣ
//const std::string &sql Ҫ��ѯ��SQL���
//std::string &sNewTable ��const std::string &TempTableEngineType = "innodb"  ��������Ҫ����ʱ����������Ϣ
//const std::string &sBeforeEndSQL = "" �Բ�ѯ�����Ը��� insert��create table��� �Ա���չ����
//const std::string &sTableWhere = "`ENGINE` = 'SPIDER'" Ϊ��Ҫ�ֲ�ִ�еĴ洢�����û��������ɵ����Ա������书��
//bool boNeedLinkTable = true �Ƿ���Ҫ���������ӣ�������б��������ݿ����账������ӣ�����ٶ�
//bool boOnlyTable = false �������SPIDER���������ݿ���ڶ�������SPIDER������ݶ��������ݿ��ڴ���������Ϊtrue

//����:�����ָ��ĳЩspider��Ҫ����ȫ�����ݣ�����ϣ���������ݿ���ֱ�Ӵ������ڱ���ǰ�ӡ�-�����м䲻������κ������ַ�����
std::string spider_pq(const std::string &sHost, const std::string &sUsername, const std::string &sPassword, int iPort, const std::string &sDatabase, const std::string &sql,
                      std::string &sNewTable, const std::string &TempTableEngineType = "innodb", const std::string &sBeforeEndSQL = "",int iMaxSQLCount = 0,
                      const std::string &sTableWhere = "`ENGINE` = 'SPIDER'",
                      const std::string &sMQFunction = "mysql.parallel_query", bool boNeedLinkTable = true, bool boOnlyTable = false)
{
  std::string sRE;

  std::string sAllSpiderTables;

  int iErrorNo = 0;
  {
    //dsp�Ĺ������� ��ʼ��ȡ����SPIDER����
    dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, sHost, sUsername, sPassword, iPort, DBMysql_CONNECTED_SQL, true);//��MySQL���ӳ���ȡһ������
    DBMysql *pMySQL = dsp.get();
    if (!pMySQL->Connected())
      {
        iErrorNo = pMySQL->DBConnect();
      }
    if (0 == iErrorNo)
      {
        iErrorNo = pMySQL->DBSelect(trim(sDatabase));
      }
    if (0 == iErrorNo)
      {
        iErrorNo =
          pMySQL->DBQuery("select CONCAT(',',GROUP_CONCAT(TABLE_SCHEMA,'.',TABLE_NAME),',') as str from information_schema.`TABLES`" + (sTableWhere.empty() ? "" : " where " + sTableWhere) +
                          " GROUP BY `ENGINE` limit 1");
      }
    if (0 == iErrorNo)
      {
        vector<strMap> All = pMySQL->GetArray();
        if (All.size() > 0)
          {
            sAllSpiderTables = All[0]["str"];
          }
        //sAllSpiderTables =
        //  ",mylx.basic_table,mylx.cifu,mylx.client_info,mylx.create_role,mylx.deal,mylx.dupl,mylx.game_buy,mylx.game_enter,mylx.game_sell,mylx.giftmoney_buy,mylx.giftmoney_income,mylx.gold,mylx.item_income,mylx.item_log,mylx.item_use,mylx.league,mylx.login,mylx.logout,mylx.map,mylx.online,mylx.recharge,mylx.retreat,mylx.shop_buy,mylx.task,mylx.upgrade,mylx.yb_expend,mylx.yb_income,";
      }
    else
      {
        sRE = "~Get AllSpiderTables MySQL ErrorNo:(" + to_string(pMySQL->GetErrorNo()) + ") Error(" + pMySQL->GetError() + ")";
      }
    pMySQL->clearGetData();
    //��ȡ����SPIDER��������
  }
  if (iErrorNo == 0)
    {
      std::string sAllSQLError = "";
      mapPRI_SQL_List newTableList;
      bool boHaveSpiderTable = false;
      std::string sMainTable;
      int iMaxPRI = 0;
      int idx = 0;
      //��ȡ�ֱ��� list
      std::string sEndSQL = fx_sql(newTableList, sql, sDatabase, sAllSpiderTables, boHaveSpiderTable, sMainTable, sNewTable, iMaxPRI, idx, boNeedLinkTable, boOnlyTable);
      std::string sNewSQL = ";" + sEndSQL;
      //std::cout << sNewTable << std::endl << sEndSQL << std::endl;
      /*
        if (newTableList[idx][sNewSQL].empty())
          {
            newTableList[idx][sNewSQL] = sNewTable;
          }
        else
          {
            sNewTable = newTableList[idx][sNewSQL];//����Ѿ��������ˣ���ֱ�������������
          }
      */
      intMap smAllTables;//��¼���е�Ҫ���ӻ��½��ı� Ҫ���ӵı�ֵΪ0(���ӵı�ֻ��Ҫ����mysql�Ͻ���ɾ�����������ϲ��ܽ���ɾ��),Ҫ�½��ı�ֵΪ1(�½��ı�ֻ��Ҫ������mysql�Ͻ���ɾ������������ҲҪɾ��)
      strlist slLinkTableSQLs;//��һִ��
      strlist slRunChildSQLs;//�ڶ�ִ��
      strlist slRunEndSQLs;//�ȴ��ڶ�ִ��ȫ����ɺ�ִ�� ������������
      ctMap ctmCreateTableSQLs;
      std::map<std::string, strMap> smAllChildCreateTables;//�����ӷ������Ͻ�������ʱ��
      std::string sServerCreateTables;//�������������Ͻ�������ʱ��
      strMap *pStrMap = nullptr;
      std::string sCreateTableTemp;
      bool boNeedCreateTable = false;//��Ȼ�׶��Ƿ���Ҫ�����±��������Ҫ��ô��ǰ�׶ξͿ��Ժ���һ�׶�һִͬ�У����Ч��
      for (int i = iMaxPRI; i >= 0; i--)//PRIֵ��ߵ���Ҫ����ִ��
        {
          boNeedCreateTable = false;
          pStrMap = &newTableList[i];
          strMap::const_iterator iter;
          for (iter = pStrMap->begin(); iter != pStrMap->end(); iter++)
            {
              //����±��ֵ��ͬ �ͱ�ʾҪ�������ݿ����½��� �������ݿ��½����Զ�����ӱ����������ݿ���ִ��mq�Ĵ洢���̽���Ľ�����뵽Զ�����ӱ� ������SQL��ѯ���Ҫ��Ԥ�ȴ���
              if (iter->second != iter->first)
                {
                  //std::cout << "Create table " << iter->second << " -> " << iter->first << ";" << std::endl;
                  intMap::iterator it = smAllTables.find(iter->second);
                  if (it == smAllTables.end())
                    {
                      smAllTables[iter->second] = 1;
                      boNeedCreateTable = true;
                      auto *OneCTM = &ctmCreateTableSQLs[iter->second];
                      OneCTM->sNewTableName = iter->second;
                      getHeadString(iter->first, ';', OneCTM->sMainTable, OneCTM->sQuerySQL);
                      //sCreateTableTemp = " ENGINE = " + TempTableEngineType + " " + GJM_split(OneCTM->sQuerySQL, "\\blimit\\b", true, 1, 1)[0] + " limit 0";

                      OneCTM->sCreateSQL = BuildCreateTableSQL(OneCTM->sQuerySQL, OneCTM->sNewTableName, TempTableEngineType, "");//"create table " + OneCTM->sNewTableName + sCreateTableTemp;
                      slLinkTableSQLs.emplace_back(OneCTM->sNewTableName);
                      OneCTM->sCreateSQL2 = BuildCreateTableSQL(OneCTM->sQuerySQL, OneCTM->sNewTableName + child_sql_table_postfix, TempTableEngineType,
                                                                "");// "create table " + OneCTM->sNewTableName + child_sql_table_postfix + sCreateTableTemp;
                      slLinkTableSQLs.emplace_back(OneCTM->sNewTableName + child_sql_table_postfix);

                      sServerCreateTables += OneCTM->sNewTableName + child_sql_table_postfix + ",";

                      mulit_query mq;
                      BuidlAllSQL_info reInfo;
                      bool boOK = mq.BuidlAllSQL(reInfo, sDatabase, OneCTM->sQuerySQL, OneCTM->sMainTable, "", 0, 1, 1, OneCTM->sNewTableName + child_sql_table_postfix, "", "",
                                                 true);
                      //reInfo.SQLs; �ֲ�ִ����� Ҫȥ��һ��ʼ�������ֺ�
                      for (auto s : reInfo.SQLs)
                        {
                          //s.substr(s.find(';') + 1);
                          slRunChildSQLs.emplace_back("call " + sMQFunction + "(database(),\"" + GJM_EscapeSQL(s.substr(s.find(';') + 1)) + "\",\"\"," + to_string(iMaxSQLCount) + ")");
                        }
                      //std::cout << "end sql:" << "INSERT INTO " + OneCTM->sNewTableName + " " + reInfo.sEndSQL << std::endl;// ����ִ�����
                      slRunEndSQLs.emplace_back(((OneCTM->sNewTableName != sNewTable) ? "INSERT INTO " + OneCTM->sNewTableName + " " : "") + reInfo.sEndSQL);
                    }
                }
              else
                {
                  //std::cout << "Link table " << iter->first << ";" << std::endl;
                  intMap::iterator it = smAllTables.find(iter->first);
                  if (it == smAllTables.end())
                    {
                      smAllTables[iter->first] = 0;
                      slLinkTableSQLs.emplace_back(iter->first);
                    }
                }
            }

          if (boNeedCreateTable)
            {
              //std::cout << "#" << to_string(i) << ":" << std::endl;
              vector<strMap> ChildServers;
              StrStrIntMap wrapperMap;
              intMap mapMainTables;
              ctMap::iterator cmtIter;
              ctMap::iterator cmtIterEnd = ctmCreateTableSQLs.end();
              strlist sCreateSQLs;
              for (cmtIter = ctmCreateTableSQLs.begin(); cmtIter != cmtIterEnd; cmtIter++)
                {
                  if (!cmtIter->second.sCreateSQL.empty())
                    {
                      sCreateSQLs.emplace_back(cmtIter->second.sCreateSQL);
                    }
                  if (!cmtIter->second.sCreateSQL2.empty())
                    {
                      sCreateSQLs.emplace_back(cmtIter->second.sCreateSQL2);
                    }
                  mapMainTables[cmtIter->second.sMainTable] = 1;
                  sServerCreateTables += cmtIter->second.sNewTableName + ",";
                }
              sAllSQLError = ThreadRunSQL(sCreateSQLs, sDatabase, sHost, sUsername, sPassword, iPort);//������������ִ��
              if (sAllSQLError != "")
                {
                  //�������ܱ�ʧ��
                  break;
                }
              strlist::iterator ltIter;
              strlist::iterator ltIterEnd = slLinkTableSQLs.end();
              SQLDBArrayList lSQLDBArray;
              int idx = 0;
              for (ltIter = slLinkTableSQLs.begin(); ltIter != ltIterEnd; ltIter++)
                {
                  lSQLDBArray[idx++].sql = "show create table " + *ltIter;
                }
              ThreadRunSQLGetField(&lSQLDBArray, sDatabase, sHost, sUsername, sPassword, iPort);

              SQLDBArrayList::iterator ltSQLIter;
              SQLDBArrayList::iterator ltSQLIterEnd = lSQLDBArray.end();
              strlist slChildRunLinkTableSQLs;
              std::string sRunLinkTableSQL;
              strlist slCreatFederatedTable;
              for (ltSQLIter = lSQLDBArray.begin(); ltSQLIter != ltSQLIterEnd; ltSQLIter++)
                {
                  if (0 == ltSQLIter->second.errorno)
                    {
                      sRunLinkTableSQL = ltSQLIter->second.data[0]["Create Table"];

                      //��ȡwrapper
                      intMap::iterator itMainTables = mapMainTables.find(ltSQLIter->second.data[0]["Table"]);
                      if (itMainTables != mapMainTables.end())//����ǰ��Ϊ����ʱ ϵͳ��鿴���Ƿ���spider�����
                        {
                          strlist matchesWrapper;
                          strlist matchesSerer;
                          int iCount = GJM_Regx("\\bENGINE\\s*?=\\s*?SPIDER\\b.*?\\bCOMMENT\\s*?=\\s*?'\\s*?wrapper\\s*?\"(.*?)\"", sRunLinkTableSQL, matchesWrapper, true, 0);
                          if (iCount > 1)//������ʵspider������׼����ȡ�����server
                            {
                              //��ȡwrapper��һ��������������
                              iCount = GJM_Regx_All("\\bPARTITION\\b.*?\\bCOMMENT\\s*?=\\s*?'.*?\\bsrv\\s*?\"([^\\s]*?)(?=[ \"])", sRunLinkTableSQL, matchesSerer, true, 0);
                              int iMatchesCount = matchesSerer.size();
                              for (int i = 0; i < iCount; i++)
                                {
                                  matchesSerer[i].erase(0, matchesSerer[i].rfind('"') + 1);
                                  wrapperMap[matchesWrapper[1]][matchesSerer[i]] = 1;
                                }
                            }
                        }
                      auto listLinkTableSQL = GJM_split(sRunLinkTableSQL, "/\\*!50100\\sPARTITION\\s", true, 2, 1);

                      slCreatFederatedTable = GJM_split((listLinkTableSQL.size() > 0 ? listLinkTableSQL[0] : sRunLinkTableSQL), " ENGINE\\s?=\\s?(.*?) ", true, 2, 1);
                      slChildRunLinkTableSQLs.emplace_back(slCreatFederatedTable[0] + " ENGINE = FEDERATED CONNECTION='mysql://" + sUsername + ":" + sPassword + "@" +
                                                           sHost +
                                                           ":" + to_string(iPort) + "/" + sDatabase + "/" + ltSQLIter->second.data[0]["Table"] + "' " + (slCreatFederatedTable.size() > 1 ? slCreatFederatedTable[1] : ""));
                    }
                  else
                    {
                      sAllSQLError += ltSQLIter->second.errorstr + ";";
                      break;
                    }
                }
              if (sAllSQLError != "")
                {
                  //��ȡ������Ҫ���ӵı����ʧ��
                  break;
                }
              // ��ʼ��ȡ spider servers
              std::string sWhere;
              int iReserve = 512;
              sWhere.reserve(iReserve);
              int iLength = 0;
              for (auto one_wrapper : wrapperMap)
                {
                  sWhere += string(sWhere.empty() ? "" : "and") + "(`Wrapper`='" + trim(one_wrapper.first) + "' and `Server_name` in(";
                  for (auto sername : one_wrapper.second)
                    {
                      sWhere += "'" + trim(sername.first) + "',";
                    }
                  iLength = sWhere.length();
                  if (sWhere[iLength - 1] == ',')
                    {
                      sWhere.erase(iLength - 1, 1);
                    }
                  sWhere += "))";
                }
              if (wrapperMap.size()>0)
                {
                  //dsp�Ĺ�������
                  dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, sHost, sUsername, sPassword, iPort, DBMysql_CONNECTED_SQL, true);//��MySQL���ӳ���ȡһ������
                  DBMysql *pMySQL = dsp.get();
                  iErrorNo = 0;
                  if (!pMySQL->Connected())
                    {
                      iErrorNo = pMySQL->DBConnect();
                    }
                  if (0 == iErrorNo)
                    {
                      iErrorNo = pMySQL->DBSelect(trim(sDatabase));
                    }
                  if (0 == iErrorNo)
                    {
                      iErrorNo = pMySQL->DBQuery("select `Server_name`,`Host`,`Db`,`Username`,`Password`,`Port`,`Socket`,`Wrapper`,`Owner` from `mysql`.`servers` where " + sWhere);
                    }
                  if (0 == iErrorNo)
                    {
                      ChildServers = pMySQL->GetArray();
                    }
                  else
                    {
                      sAllSQLError = "Get `mysql`.`servers` MySQL ErrorNo:(" + to_string(pMySQL->GetErrorNo()) + ") Error(" + pMySQL->GetError() + ")";
                    }
                  pMySQL->clearGetData();
                }
              // ��ȡ spider servers ����

              //��ִ�����ӱ���� ��ִ��slChildRunLinkTableSQLs
              strlist::iterator Iter;
              strlist::iterator IterEnd = slChildRunLinkTableSQLs.end();
              strlist slAllRetrun;
              int iRowCount = ChildServers.size();
              for (int i = 0; i < iRowCount; i++)
                {
                  slAllRetrun.emplace_back("");
                }
              ThreadRunMulitLinkTableSQL(&slAllRetrun, slChildRunLinkTableSQLs, &ChildServers);//���Ⲣ��
              strlist::iterator slIter;
              strlist::iterator slIterEnd = slAllRetrun.end();
              for (slIter = slAllRetrun.begin(); slIter != slIterEnd; slIter++)
                {
                  if (!slIter->empty())
                    {
                      sAllSQLError += (*slIter) + ";";
                    }
                }
              if (sAllSQLError != "")
                {
                  //��ȡ������Ҫ���ӵı����ʧ��
                  break;
                }
              //��ʼ��¼�����ӷ�������������ʱ��
              for (int i = 0; i < iRowCount; i++)
                {
                  auto it = smAllChildCreateTables.find(ChildServers[i]["Server_name"]);
                  if (it == smAllChildCreateTables.end())
                    {
                      smAllChildCreateTables[ChildServers[i]["Server_name"]] = ChildServers[i];
                    }
                  else
                    {
                      smAllChildCreateTables[ChildServers[i]["Server_name"]][CreateTableNamesFieldName] += ChildServers[i][CreateTableNamesFieldName];
                    }
                }

              //����ִ���ӷ�������ѯ���
              slAllRetrun.clear();
              for (int i = 0; i < iRowCount; i++)
                {
                  slAllRetrun.emplace_back("");
                }
              ThreadRunMulitSQL(&slAllRetrun, slRunChildSQLs, ChildServers);
              slIterEnd = slAllRetrun.end();
              for (slIter = slAllRetrun.begin(); slIter != slIterEnd; slIter++)
                {
                  if (!slIter->empty())
                    {
                      sAllSQLError += (*slIter) + ";";
                    }
                }
              if (sAllSQLError != "")
                {
                  //��ȡ������Ҫ���ӵı����ʧ��
                  break;
                }

              //ִ�з������ϵĲ�ѯ���
              sAllSQLError = ThreadRunSQL(slRunEndSQLs, sDatabase, sHost, sUsername, sPassword, iPort);//������������ִ��
              if (sAllSQLError != "")
                {
                  //�������ܱ�ʧ��
                  break;
                }

              //��ʼ������ִ�е����� Ϊ�´�ִ����׼��
              slLinkTableSQLs.clear();
              slRunChildSQLs.clear();//�ڶ�ִ��
              slRunEndSQLs.clear();//�ȴ��ڶ�ִ��ȫ����ɺ�ִ�� ������������
              ctmCreateTableSQLs.clear();
            }
        }

      ThreadRunMulitDropTableSQL(smAllChildCreateTables);//��������ӷ����������ı���ʱ��
      if (sServerCreateTables.length() > 0)
        {
          //ɾ�����������Ͻ�������ʱ��
          //dsp�Ĺ�������
          dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, sHost, sUsername, sPassword, iPort, DBMysql_CONNECTED_SQL, true);//��MySQL���ӳ���ȡһ������
          DBMysql *pMySQL = dsp.get();
          iErrorNo = 0;
          if (!pMySQL->Connected())
            {
              iErrorNo = pMySQL->DBConnect();
            }
          if (0 == iErrorNo)
            {
              iErrorNo = pMySQL->DBSelect(trim(sDatabase));
            }
          if (0 == iErrorNo)
            {
              iErrorNo = pMySQL->DBQuery("create table " + sNewTable + " " + sEndSQL);
            }
          if (0 == iErrorNo)
            {
              iErrorNo = pMySQL->DBQuery("drop table " + trim(sServerCreateTables, SPACE_CHARACTERS + ","));
            }
          if (0 != iErrorNo)
            {
              sAllSQLError += "~Get MySQL ErrorNo:(" + to_string(pMySQL->GetErrorNo()) + ") Error(" + pMySQL->GetError() + ")";
            }
          pMySQL->clearGetData();
        }
      if (sAllSQLError.empty())
        {
          sRE = " " + sNewTable;
        }
      else
        {
          sRE = "~" + sAllSQLError;
        }
    }

  return std::move(sRE);
}

extern "C" char *run_spider_pq(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;
  int port = 3306;
  std::string sNewTable = "TempTable";
  if (args->arg_count > 6)              //����������
    {
      sNewTable = trim(args->args[6]);
      if (sNewTable.empty())
        {
          sNewTable = "TempTable";
        }
    }
  std::string sTempTableEngineType = "InnoDB";
  if (args->arg_count > 7)              //����������
    {
      sTempTableEngineType = trim(args->args[7]);
      if (sTempTableEngineType.empty())
        {
          sTempTableEngineType = "InnoDB";
        }
    }

  std::string sBeforeEndSQL = "";
  if (args->arg_count > 8)              //����������
    {
      sBeforeEndSQL = args->args[8];
    }
  int iMaxSQLCount = 0;
  if (args->arg_count > 9)              //����������
    {
      iMaxSQLCount = (int)* ((long long *)args->args[9]);
    }
  std::string sTableWhere = "`ENGINE` = 'SPIDER'";
  if (args->arg_count > 10)              //����������
    {
      sTableWhere = trim(args->args[10]);
      if (sTableWhere.empty())
        {
          sTableWhere = "`ENGINE` = 'SPIDER'";
        }
    }
  std::string sMQFunction = "mysql.parallel_query";
  if (args->arg_count > 11)              //����������
    {
      sMQFunction = trim(args->args[11]);
      if (sMQFunction.empty())
        {
          sMQFunction = "mysql.parallel_query";
        }
    }
  bool boNeedLinkTable = true;
  if (args->arg_count > 12)              //����������
    {
      boNeedLinkTable = ((int)* ((long long *)args->args[12])==0)?false:true;
    }
  bool boOnlyTable = false;
  if (args->arg_count > 13)              //����������
    {
      boOnlyTable = ((int)* ((long long *)args->args[13]) == 0) ? false : true;
    }

  std::string  sRE = spider_pq(args->args[0], args->args[1], args->args[2], (int)* ((long long *)args->args[3]),args->args[4],
                               args->args[5], sNewTable, sTempTableEngineType, sBeforeEndSQL, iMaxSQLCount, sTableWhere, sMQFunction,
                               boNeedLinkTable, boOnlyTable);

  *res_length = sRE.length();
  initid->ptr = nullptr;
  if (*res_length > 255)
    {
      initid->ptr = new char[*res_length + 1];
      memcpy(initid->ptr, sRE.c_str(), *res_length);
      initid->ptr[*res_length] = '\0';
      return initid->ptr;
    }
  else
    {
      memcpy(result, sRE.c_str(), *res_length);
      result[*res_length] = '\0';
      return result;
    }
}

extern "C" my_bool run_spider_pq_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if ((args->arg_count < 6) || (args->arg_count > 14))              //����������
    {
      strcpy(message, "run_spider_pq Greater than or equal 6 parameters");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //����������
    {
      strcpy(message, "Host parameter must be a string type");
      return 1;
    }
  if (args->arg_type[1] != STRING_RESULT)    //����������
    {
      strcpy(message, "Username parameter must be a string type");
      return 1;
    }
  if (args->arg_type[2] != STRING_RESULT)    //����������
    {
      strcpy(message, "Password parameter must be a string type");
      return 1;
    }
  if (args->arg_type[3] != INT_RESULT)    //����������
    {
      strcpy(message, "Port value parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[4] != STRING_RESULT)    //����������
    {
      strcpy(message, "Database parameter must be a string type");
      return 1;
    }
  if (args->arg_type[5] != STRING_RESULT)    //����������
    {
      strcpy(message, "Query SQL parameter must be a string type");
      return 1;
    }
  if ((args->arg_count > 6)&&(args->arg_type[6] != STRING_RESULT))    //����������
    {
      strcpy(message, "New Table parameter must be a string type");
      return 1;
    }
  if ((args->arg_count > 7) && (args->arg_type[7] != STRING_RESULT))    //����������
    {
      strcpy(message, "Temp Table Engine Type parameter must be a string type");
      return 1;
    }
  if ((args->arg_count > 8) && (args->arg_type[8] != STRING_RESULT))    //����������
    {
      strcpy(message, "Before End Query SQL parameter must be a string type");
      return 1;
    }
  if ((args->arg_count > 9) && (args->arg_type[9] != INT_RESULT))    //����������
    {
      strcpy(message, "Max SQL Count SQL parameter must be a integer type");
      return 1;
    }
  if ((args->arg_count > 10) && (args->arg_type[10] != STRING_RESULT))    //����������
    {
      strcpy(message, "Spider Table Where SQL parameter must be a string type");
      return 1;
    }
  if ((args->arg_count > 11) && (args->arg_type[11] != STRING_RESULT))    //����������
    {
      strcpy(message, "Parallel query function name parameter must be a string type");
      return 1;
    }
  if ((args->arg_count > 12) && (args->arg_type[12] != INT_RESULT))    //����������
    {
      strcpy(message, "Need Link Table parameter must be a integer type");
      return 1;
    }
  if ((args->arg_count > 13) && (args->arg_type[13] != INT_RESULT))    //����������
    {
      strcpy(message, "Only Table parameter must be a integer type");
      return 1;
    }
  return 0;
}
extern "C" void run_spider_pq_deinit(UDF_INIT *initid)
{
  if (nullptr != initid->ptr)
    {
      delete[](initid->ptr);
      initid->ptr = nullptr;
    }
}
