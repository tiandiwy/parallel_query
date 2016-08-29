// lx.cpp : 定义控制台应用程序的入口点。

#include "stdafx.h"
#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <regex>
#include <thread>
#include <mutex>
#include <chrono>

#ifdef _MSC_VER
  #include <winsock2.h>
  #include <mysql.h>
  #include <DBMysql.h>
  #include <DBMysql_Pool.hpp>
  #pragma comment(lib,"wsock32.lib")
  #pragma comment(lib,"libmysql.lib")
#else
  #include <mysql/mysql.h>
  #include "DBMysql/DBMysql.h"
  #include "DBMysql/DBMysql_Pool.hpp"
#endif

//标准通用函数 开始
const std::string SPACE_CHARACTERS = " \t\r\n\x0B";
typedef std::vector<std::string> strlist;
const std::string MQ_COUNT_FIELD = "MQ_1MQ_COUNT_2FIELD3";

//in_str：输一长串入的字符串 src：准备要被替换的小字符串 dest：准备要替换为的结果字符串
//返回值：替换后的整体输出字符串
std::string StrReplace(std::string in_str, const std::string &src, const std::string &dest)
{
  int pos = in_str.find(src);//find表示在in_str内查找src_str
  if (pos != string::npos) //string::npos表示没查到
    {
      in_str = in_str.replace(pos, src.length(), dest);//如果查到了就用函数replace替换
      //replace的一参表示要替换起始位置，二参表示要替换的长度，三参表示要替换成什么
    }

  return std::move(in_str);
}

bool StrReplace2(std::string &in_str,const std::string &src, const std::string &dest)
{
  bool boRE = false;
  int pos = in_str.find(src);//find表示在in_str内查找src_str
  if (pos != string::npos) //string::npos表示没查到
    {
      boRE = true;
      in_str = in_str.replace(pos, src.length(), dest);//如果查到了就用函数replace替换
      //replace的一参表示要替换起始位置，二参表示要替换的长度，三参表示要替换成什么
    }

  return boRE;
}

//boNesting为0时不忽略 boNesting为1 时忽略嵌套中的内容 (),",',` boNesting为2 时忽略嵌套中的内容 (),",'
std::vector<std::string> GJM_split(const std::string &str,const std::string &sReg, bool icase = true, int iMaxCount = 0, int iNesting = 1)
{
  int iRCount = 0;
  std::string NewStr;
  std::string sNesting;
  std::vector<std::string> slRStr;

  std::string *pStr = (std::string *)&str;

  if (iNesting>0)
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
                          NewStr += ("<$" + to_string(iRCount++) + "$>");
                          slRStr.push_back(sNesting);
                        }
                    }
                  break;
                }
              case '\'':
              case '"':
              case '`':
                {
                  if ((iNesting == 2)&&(ch == '`'))
                    {
                      if (iH <= 0) { NewStr += ch; }
                      else { sNesting += ch; }
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
                          NewStr += ("<$" + to_string(iRCount++) + "$>");
                          slRStr.push_back(sNesting);
                        }
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0) { NewStr += ch; }
                  else { sNesting += ch; }
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
      if (iNesting>0)
        {
          sNesting = *it++;
          for (; idx < iRCount; idx++)
            {
              if (!StrReplace2(sNesting, "<$" + to_string(idx) + "$>", slRStr[idx]))
                {
                  break;
                }
            }
          vec.push_back(sNesting);
        }
      else
        {
          vec.push_back(*it++);
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

  if (iNesting>0)
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
                          NewStr += ("<$" + to_string(iRCount++) + "$>");
                          slRStr.push_back(sNesting);
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
                      if (iH <= 0) { NewStr += ch; }
                      else { sNesting += ch; }
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
                          NewStr += ("<$" + to_string(iRCount++) + "$>");
                          slRStr.push_back(sNesting);
                        }
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0) { NewStr += ch; }
                  else { sNesting += ch; }
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
              if (iNesting>0)
                {
                  sNesting = sResult[i].str();
                  for (int idx = 0; idx < iRCount; idx++)
                    {
                      StrReplace2(sNesting, "<$" + to_string(idx) + "$>", slRStr[idx]);
                    }
                  Result.push_back(sNesting);
                }
              else
                {
                  Result.push_back(sResult[i].str());
                }
              iRE++;
            }
        }
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

  if (iNesting>0)
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
                          NewStr += ("<$" + to_string(iRCount++) + "$>");
                          slRStr.push_back(sNesting);
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
                      if (iH <= 0) { NewStr += ch; }
                      else { sNesting += ch; }
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
                          NewStr += ("<$" + to_string(iRCount++) + "$>");
                          slRStr.push_back(sNesting);
                        }
                    }
                  break;
                }
              default:
                {
                  if (iH <= 0) { NewStr += ch; }
                  else { sNesting += ch; }
                  break;
                }
            }
        }
      pStr = &NewStr;
    }

  regex_constants::syntax_option_type r_type = icase ? regex_constants::icase : regex_constants::ECMAScript;
  std::regex exp(sRegx, r_type);
  sNesting = std::regex_replace(*pStr, exp, replacer);
  if (iNesting>0)
    {
      for (int idx = 0; idx < iRCount; idx++)
        {
          StrReplace2(sNesting, "<$" + to_string(idx) + "$>", slRStr[idx]);
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
//标准通用函数 结束

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

DBMysql_Pool<DBMysql> MySQLPool(40);

void ThreadRun(strlist *slAllSQL, int iIndex, const std::string &database, const std::string &sql, const std::string &host, const std::string &user, const std::string &password,
               unsigned int port = 3306)
{
  int iAllSQLCount = slAllSQL->size();
  if ((iAllSQLCount > iIndex) && (iIndex >= 0))//iIndex必须正确
    {

      dbmysql_scoped_popres<DBMysql> dsp(MySQLPool, host, user, password, port, true);
      DBMysql *pMySQL = dsp.get();
      //DBMysql myDB(host, user, password, port);
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

      /*
      DBMysql myDB(host, user, password, port);
      int iErrorNo = myDB.GetErrorNo();
      if (0 == iErrorNo)
      {
      iErrorNo = myDB.DBConnect();
      }
      if (0 == iErrorNo)
      {
      iErrorNo = myDB.DBSelect(database);
      }
      if (0 == iErrorNo)
      {
      iErrorNo = myDB.DBQuery(sql);
      }
      if (0 == iErrorNo)
      {
      (*slAllSQL)[iIndex] = "+" + to_string(myDB.GetNum());
      }
      if (iErrorNo > 0)
      {
      iErrorNo = 0 - iErrorNo;
      }
      if (iErrorNo != 0)
      {
      (*slAllSQL)[iIndex] = " " + to_string(myDB.GetErrorNo()) + " " + myDB.GetError();
      }
      */

    }
}

class mulit_query
{
  protected:
    std::string Orig_SQL;//原查询SQL

    void ClearVar(void)//初始化整个对象
    {
      Orig_SQL = ""; //原查询SQL
    }

  public://get set 函数群
    inline std::string getOrig_SQL(void)
    {
      return Orig_SQL;
    }
    inline void setOrig_SQL(const std::string &mOrig_SQL)
    {
      Orig_SQL = mOrig_SQL;
    }

  public://构造与析构函数
    mulit_query(void)
    {
      ClearVar();
    }

    ~mulit_query()
    {
      ClearVar();
    }

  public:
    //sMainForm 如果没有别名就是主表名 否则 就应该是主表的别名
    bool BuidlAllSQL(BuidlAllSQL_info &reInfo, const std::string &sql, const std::string sMainForm, const std::string &sPRIField, int iMinPRIValue, int iMaxPRIValue, int MaxSQLCount,
                     const std::string &TempName, const std::string &EndSQL = "")
    {
      reInfo.Clear();

      std::string tmpCount = "";
      map<std::string, std::string> strFieldMAP;

      Orig_SQL = sql;
      auto DeSQL = DecomposeSQL(sql); //先分解SQL语句
      if (DeSQL.boStatus)
        {
          reInfo.boStatus = true;
          reInfo.isSelect = DeSQL.isSelect;
        }
      else
        {
          reInfo.iErrNo = -2;
          reInfo.sErrStr = "No from definition";
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
                      sFields += ','; //末尾添加,符号是为了让程序永远知道最后一个字段结束了
                    }
                  int iFSLen = sFields.length();
                  std::string sFieldsOK = "";
                  sFieldsOK.reserve(256);
                  int iKH = 0; //括弧计数
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
                  bool isGruopFunc = false; //是否为汇总函数
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
                          case ',': //表示当前字段结束
                            {
                              if (iKH > 0) //如果还在括号内，表示字段还没有结束，这个','是函数参数分隔符
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
                                          //    //如果是汇总函数就要看函数的参数是不是含有操作符
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
                                          //    //只要不是汇总函数，都必须要露这个字段的原本定义
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
                              if (!InAsName) //没有别名中 并且不是在汇总函数中
                                {
                                  if ((sFuncName.length() > 0) && isGruopFunc)
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
                            {
                              if (!InAsName)
                                {
                                  sOneField += ch;
                                  std::string sOF = rtrim(sOneField);
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
                  std::string SelectSQL = rtrim(sFieldsOK, ","); //将所有count函数转为sum函数
                  std::string EndSQL = TempName + " " + (DeSQL.sSQL_End_All.empty() ? DeSQL.sSQL_End : DeSQL.sSQL_End_All)
                                       + (DeSQL.sSQL_Limit.empty() ? "" : " limit " + DeSQL.sSQL_Limit);
                  EndSQL = GJM_RegxReplace("\\b(?:\\s{0,}(?:`\\w{1,}`|\\w{1,})\\s{0,}\\.|\\s{0,}(?:`\\w{1,}`|\\w{1,})\\s{0,}\\.\\s{0,}(?:`\\w{1,}`|\\w{1,})\\s{0,}\\.)\\s{0,}(`\\w{1,}`|\\w{1,})\\s{0,1}(?=\\,{0,1})",
                                           " $1 ", EndSQL,true,2);//将带有表名（或带有数据库名）的字段名，去掉表名（或数据库名）
                  reInfo.sEndSQL = "Select " + SelectSQL + " from " + EndSQL;
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
                  reInfo.sEndSQL = EndSQL;
                }
            }

          int iMaxCount = iMaxPRIValue - iMinPRIValue;
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
                  //循环的最后一次
                  iEnd = iMaxPRIValue;
                }
              std::string sWhereSQL = sMainForm + "." + sPRIField + " >= " + to_string(iBengin) + " and " + sMainForm + "." + sPRIField + " <= " + to_string(iEnd);
              std::string sInsertInto = DeSQL.isSelect ? ("INSERT INTO " + TempName + " ") : "";
              reInfo.SQLs.push_back(sInsertInto + DeSQL.sSQL_Select + (DeSQL.isUpdate?" set ":" from ") + DeSQL.sSQL_From + " where "
                                    + (DeSQL.sSQL_Where.empty() ? sWhereSQL : sWhereSQL + " and (" + DeSQL.sSQL_Where + ")")
                                    + " " + DeSQL.sSQL_End + (DeSQL.sSQL_Limit.empty() ? "" : " limit " + DeSQL.sSQL_Limit));
            }
        }
      return reInfo.boStatus;
    }
    std::string Run(strlist &slAllSQL, const std::string &database, const std::string &host, const std::string &user, const std::string &password,
                    unsigned int port = 3306)
    {
      int iSQLCount = slAllSQL.size();
      std::vector<std::thread> threads;
      for (int i = 0; i < iSQLCount; i++)
        {
          threads.push_back(std::thread(std::bind(ThreadRun, &slAllSQL, i, database, slAllSQL[i], host, user, password, port)));
        }
      for (auto &thread : threads)
        {
          thread.join();
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

    bool StrEndIsNum(const std::string &sStr) //检测最后一个有效内容是否为纯数字
    {
      std::regex r("([^\\s]{1,}?)(?=\\b|$)");
      std::string EndStr;

      for (std::sregex_iterator it(sStr.begin(), sStr.end(), r), end; it != end; ++it)//end是尾后迭代器，regex_iterator是regex_iterator的string类型的版本
        {
          EndStr = it->str();
        }
      std::regex r2("^[\\d]{1,}$");
      return std::regex_match(EndStr, r2);//return std::regex_search(EndStr, r2);
    }

    DecomposeSQL_info DecomposeSQL(std::string sql) //分解SQL语句
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
      if (RE.isSelect)
        {
          from_s = GJM_split(sql, "\\bfrom\\b", true, 2); //preg_split('/\bfrom\b/i', $sql, 2);
        }
      if (RE.isUpdate)
        {
          from_s = GJM_split(sql, "\\bset\\b", true, 2); //preg_split('/\bfrom\b/i', $sql, 2);
        }
      if (from_s.size() == 2)
        {
          //有from 关键字
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
              //有where关键字
              RE.sSQL_From = where_s[0];

              //preg_match_all('/(.*?)(?:\border\b|\bgroup\b|\blimit\b)/i', $where_s[1], $matches);
              strlist matches;
              int iCount = GJM_Regx("(.*?)(?:\\border\\b|\\bwith\\b|\\bgroup\\b|\\blimit\\b)", where_s[1], matches, true,1);

              if (iCount <= 0)
                {
                  //where 后面没有关键字了
                  //$RE['sql']['where'] = $where_s[1];
                  RE.sSQL_Where = where_s[1];
                }
              else
                {
                  //$matches[1][0] 就是 from 内容
                  //$RE['sql']['where'] = $matches[1][0];
                  RE.sSQL_Where = matches[1];
                  //$RE['sql']['end'] = substr($where_s[1], strlen($matches[1][0])); //就可以还原SQL
                  RE.sSQL_End = where_s[1].substr(matches[1].length(), where_s[1].length() - matches[1].length()); //就可以还原SQL
                }
            }
          else
            {
              //没有where关键字
              //preg_match_all('/(.*?)(?:\bleft\b|\bright\b|\binner\b|\bcross\b|\border\b|\bgroup\b|\blimit\b)/i',$from_s[1], $matches);
              strlist matches;
              int iCount = GJM_Regx("(.*?)(?:\\border\\b|\\bwith\\b|\\bgroup\\b|\\blimit\\b)", from_s[1], matches, true,1);
              if (iCount <= 0)
                {
                  //没有其他部分
                  //$RE['sql']['from'] = $from_s[1];
                  RE.sSQL_From = from_s[1];
                }
              else
                {
                  //$matches[1][0] 就是 from 内容
                  //$RE['sql']['from'] = $matches[1][0];
                  RE.sSQL_From = matches[1];
                  //$RE['sql']['end'] = substr($from_s[1], strlen($matches[1][0]));
                  RE.sSQL_End = from_s[1].substr(matches[1].length(), from_s[1].length() - matches[1].length());
                }
            }
        }
      else
        {
          //没有from定义
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
              //开始处理 limit
              //preg_match_all('/limit (\d{1,}\s{0,}\,{0,1}\s{0,}\d{0,}\s{0,})$/i', $RE['sql']['end'], $matches);
              strlist matches;
              int iCount = GJM_Regx("limit (\\d{1,}\\s{0,}\\,{0,1}\\s{0,}\\d{0,}\\s{0,})$", RE.sSQL_End, matches, true,1);

              if (iCount > 0)
                {
                  //$RE['sql']['limit'] = $matches[1][0];
                  RE.sSQL_Limit = matches[1];
                  //$RE['sql']['end'] = substr($RE['sql']['end'], 0, 0 - strlen($matches[0][0]));
                  RE.sSQL_End = RE.sSQL_End.substr(0, RE.sSQL_End.length() - matches[0].length());
                }

              //$RE['sql']['end_all'] = $RE['sql']['end'];
              RE.sSQL_End_All = RE.sSQL_End;
              //开始处理 having
              //preg_match_all('/\bgroup\sby\b.*?\b(having.*?)(?=\border\b|\blimit\b|$)/i', $RE['sql']['end'], $matches);
              matches.clear();
              iCount = GJM_Regx("\\bgroup\\sby\\b.*?\\b(having.*?)(?=\\border\\b|\\bwith\\b|\\blimit\\b|$)", RE.sSQL_End, matches, true,1);
              if (iCount > 0)
                {
                  //RE.sSQL_End = preg_replace('/(\bgroup\sby\b.*?)\bhaving\b.*?(?=\border\b|\blimit\b|$)/i', '$1', $RE['sql']['end']);
                  RE.sSQL_End = GJM_RegxReplace("(\\bgroup\\sby\\b.*?)\\bhaving\\b.*?(?=\\border\\b|\\bwith\\b|\\blimit\\b|$)", "$1", RE.sSQL_End,true,1);
                }
              //开始处理 with
              matches.clear();
              iCount = GJM_Regx("\\bgroup\\sby\\b.*?\\b(with.*?)(?=\\border\\b|\\blimit\\b|$)", RE.sSQL_End, matches, true,1);
              if (iCount > 0)
                {
                  //RE.sSQL_End = preg_replace('/(\bgroup\sby\b.*?)\bhaving\b.*?(?=\border\b|\blimit\b|$)/i', '$1', $RE['sql']['end']);
                  RE.sSQL_End = GJM_RegxReplace("(\\bgroup\\sby\\b.*?)\\bwith\\b.*?(?=\\border\\b|\\blimit\\b|$)", "$1", RE.sSQL_End,true,1);
                }
            }
          else
            {
              //没有from定义
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
  if (args->arg_count != 1)              //检查参数个数
    {
      strcpy(error, "is_select_sql() can onlyaccept one argument");
      return 0;
    }

  if (args->arg_type[0] != STRING_RESULT)    //检查参数类型
    {
      strcpy(error, "is_select_sql() argumenthas to be an string");
      return 0;
    }
  return isSelectSQL(args->args[0]) ? 1 : 0;//0是错误 1是正确
}
extern "C" my_bool is_select_sql_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  if (args->arg_count != 1)              //检查参数个数
    {
      strcpy(message, "is_select_sql() can onlyaccept one argument");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "is_select_sql() argumenthas to be an string");
      return 1;
    }
  return 0;
}

bool getMainFrom(const std::string &sql, std::string &sMainName, std::string &sAsName)
{
  bool boRE = false;
  bool boQDF = false;//是否含有前导符 `
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
          int iCount = GJM_Regx("^as\\s(.{1,})(?=\\s|\\b|$)", sAN, Result,1);
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
  if (args->arg_count != 1)              //检查参数个数
    {
      strcpy(error, "get_main_from() can onlyaccept one argument");
      *null_value = 1;
      return nullptr;
    }

  if (args->arg_type[0] != STRING_RESULT)    //检查参数类型
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
  if (args->arg_count != 1)              //检查参数个数
    {
      strcpy(message, "get_main_from() can onlyaccept one argument");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //检查参数类型
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

//string EngineType;// = "innodb"; //表的存储引擎类型 create temporary table aa ENGINE=MEMORY (select * from recharge limit 0) 还有可能是“federated”
//string EngineConnection;// 在EngineType为"federated"的情况下需要设置Connection参数。
std::string BuildCreateTableSQL(std::string sql, const std::string &TempName, const std::string &EngineType = "innodb", const std::string &EngineConnection = "")
{
  const std::string sCreate = "CREATE TABLE " + TempName + " ENGINE=" + EngineType + " " + (EngineConnection.empty() ? "" : (" Connection='" + EngineConnection + "'"));
  sql = GJM_RegxReplace("(\\bselect\\b.*?avg\\b\\(.*?\\).*?)(?=from\\b)", "$1,count(*) as " + MQ_COUNT_FIELD + " ", sql,true,1);

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
  if ((args->arg_count != 2) && (args->arg_count != 4))              //检查参数个数
    {
      strcpy(error, "build_create_table_sql() 2 or 4 argumenthas");
      *null_value = 1;
      return nullptr;
    }
  for (unsigned int i = 0; i < args->arg_count; i++)
    {
      if (args->arg_type[i] != STRING_RESULT)    //检查参数类型
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
  if ((args->arg_count != 2) && (args->arg_count != 4))              //检查参数个数
    {
      strcpy(message, "build_create_table_sql() 2 or 4 argumenthas");
      return 1;
    }
  for (unsigned int i = 0; i < args->arg_count; i++)
    {
      if (args->arg_type[i] != STRING_RESULT)    //检查参数类型
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

std::string RunMultiQuery(const std::string &database, const std::string &sql, const std::string sMainForm,
                          const std::string &sPRIField, int iMinPRIValue, int iMaxPRIValue, int iMaxThreadCount,
                          const std::string &TempName, const std::string &EndSQL,
                          const std::string &host, const std::string &user, const std::string &password,
                          unsigned int port = 3306)
{
  mulit_query mq;
  BuidlAllSQL_info reInfo;
  bool boOK = mq.BuidlAllSQL(reInfo, sql, sMainForm, sPRIField, iMinPRIValue, iMaxPRIValue, iMaxThreadCount, TempName, EndSQL);
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

extern "C" char *run_multi_query(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;
  int port = 3306;
  if (args->arg_count == 13)              //检查参数个数
    {
      port = (int) * ((long long *)args->args[12]);
    }

  std::string  sRE = RunMultiQuery(args->args[0], args->args[1], args->args[2], args->args[3],
                                   (int) * ((long long *)args->args[4]), (int) * ((long long *)args->args[5]), (int) * ((long long *)args->args[6]),
                                   args->args[7], args->args[8], args->args[9], args->args[10], args->args[11], port);

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
extern "C" my_bool run_multi_query_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if ((args->arg_count != 12) && (args->arg_count != 13))              //检查参数个数
    {
      strcpy(message, "build_create_table_sql() 12 or 13 argumenthas");
      return 1;
    }

  if (args->arg_type[0] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "Database parameter must be a string type");
      return 1;
    }
  if (args->arg_type[1] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "SQL parameter must be a string type");
      return 1;
    }
  if (args->arg_type[2] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "Main Form parameter must be a string type");
      return 1;
    }
  if (args->arg_type[3] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "PRI Field parameter must be a string type");
      return 1;
    }
  if (args->arg_type[4] != INT_RESULT)    //检查参数类型
    {
      strcpy(message, "Min PRI value parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[5] != INT_RESULT)    //检查参数类型
    {
      strcpy(message, "Max PRI value parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[6] != INT_RESULT)    //检查参数类型
    {
      strcpy(message, "Max thread count parameter must be a integer type");
      return 1;
    }
  if (args->arg_type[7] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "Temp table name parameter must be a string type");
      return 1;
    }
  if (args->arg_type[8] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "End SQL parameter must be a string type");
      return 1;
    }
  if (args->arg_type[9] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "Host parameter must be a string type");
      return 1;
    }
  if (args->arg_type[10] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "User parameter must be a string type");
      return 1;
    }
  if (args->arg_type[11] != STRING_RESULT)    //检查参数类型
    {
      strcpy(message, "Password parameter must be a string type");
      return 1;
    }
  if (args->arg_count == 13)
    {
      if (args->arg_type[12] != INT_RESULT)    //检查参数类型
        {
          strcpy(message, "Port parameter must be a integer type");
          return 1;
        }
    }
  return 0;
}
extern "C" void run_multi_query_deinit(UDF_INIT *initid)
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
  return myMilliseconds();//0是错误 1是正确
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
  return myMicroseconds();//0是错误 1是正确
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
  return myNanoseconds();//0是错误 1是正确
}
extern "C" my_bool my_nanoseconds_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  return 0;
}

extern "C" long long mysql_pool_size(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
  initid->ptr = nullptr;
  int iMax = 0;
  if (args->arg_count == 1)              //检查参数个数
    {
      if (args->arg_type[0] == INT_RESULT)    //检查参数类型
        {
          iMax = (int) * ((long long *)args->args[0]);
        }
    }
  if (iMax > 0)
    {
      MySQLPool.setmax(iMax);
    }
  return MySQLPool.getmax();
}

extern "C" my_bool mysql_pool_size_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  if (args->arg_count > 1)              //检查参数个数
    {
      strcpy(message, "mysql_pool_size() 0 or 1 argumenthas");
      return 1;
    }
  if (args->arg_count == 1)              //检查参数个数
    {
      if (args->arg_type[0] != INT_RESULT)    //检查参数类型
        {
          strcpy(message, "pool size parameter must be a integer type");
          return 1;
        }
    }
  return 0;
}

extern "C" char *multi_query_version(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *res_length, char *null_value, char *error)
{
  initid->ptr = nullptr;

  *res_length = 7;
  memcpy(result, "0.5.0.1", *res_length);
  result[*res_length] = '\0';
  return result;

}
extern "C" my_bool multi_query_version_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  initid->ptr = nullptr;
  return 0;
}
