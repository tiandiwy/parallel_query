//---------------------------------------------------------------------------
/*��Դ���� ֻ�Կ�����Դ���б�������ʹ���е���Դû�������� ֻ������         */
//---------------------------------------------------------------------------
#ifndef DBMysql_PoolHPP
#define DBMysql_PoolHPP
//---------------------------------------------------------------------------
#include <mutex>
#include <string>
#include <queue>
#include <map>
#ifdef _MSC_VER
  #include <DBMysql.h>
  #include <gjm_m_respool.hpp>
  //#include<windows.h>
#else
  #include "DBMysql.h"
  #include "gjm_m_respool.hpp"
#endif
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
template <typename res>
//---------------------------------------------------------------------------
class DBMysql_Pool
{
  private:
    char KeyFGF = ';';

    mutex write_mutex;//�޸���
    mutex pop_mutex;//ֹͣ��ȡ��Դ��

    map<string, gjm_m_respool<res>*> Pool;

#ifdef _MSC_VER
    inline void mysleep(unsigned long ul)
    {
      Sleep(ul);
    };
#else
    inline void mysleep(unsigned long ul)
    {
      usleep(ul * 1000);
    };
#endif

    unsigned int max;
    void clear(void)//�����Դ��
    {
      max = 0;
      for (auto &kv : Pool)
        {
          if (kv.second != nullptr)
            {
              delete kv.second;
              kv.second = nullptr;
            }
        }
      Pool.clear();
    }

  protected:
  public:
    inline DBMysql_Pool(void)
    {
      clear();
    }

    inline DBMysql_Pool(unsigned int imax)
    {
      clear();
      max = imax;
    }

    inline virtual ~DBMysql_Pool(void)
    {
      clear();
    }

    inline unsigned int getCount(void)//��ǰ��Դ����
    {
      unsigned int count = 0;
      for (auto &kv : Pool)
        {
          count += kv.second->getCount();
        }
      return count;
    }

    inline unsigned int idle_count(void)//��ǰ������Դ����
    {
      unsigned int idleres = 0;
      for (auto &kv : Pool)
        {
          idleres += kv.second->idle_count();
        }
      return idleres;
    }

    inline unsigned int use_count(void)//��ǰʹ������Դ����
    {
      unsigned int useres = 0;
      for (auto &kv : Pool)
        {
          useres += kv.second->use_count();
        }
      return useres;
    }

    void setmax(unsigned int new_max)//���������Դ��
    {
      std::lock_guard<std::mutex> lock(write_mutex);//ֻ��Ҫ��һ���Ϳ����ˣ���Ϊ�������ڷ���û�п�����Դֻ���˳����ɣ�����ȴ��µ���Դ
      for (auto &kv : Pool)
        {
          kv.second->setmax(new_max);//��ÿ������Դ���븸��Դ����������ͬ
        }
      max = new_max;
    }

    inline unsigned int getmax(void)
    {
      return max; //��ȡ�����Դ��
    }

    bool empty(void)
    {
      max = 0 ? true : false;  //��ǰ��Դ���Ƿ�Ϊ��
    }

    res *pop(const string &host, const string &user, const string &password, unsigned int port,bool boWait = true)//ȡ����Դ
    {
      res *re = nullptr;
      if (max <= 0) { return nullptr; }
      std::lock_guard<std::mutex> lock_pop(pop_mutex);//�ȶ�pop��ס
      string sKey = host + KeyFGF + user + KeyFGF + password + KeyFGF + to_string(port);

      do
        {
          {
            //����������֤write_mutex����mysleep����֮ǰ�Զ��⿪
            std::lock_guard<std::mutex> lock_write(write_mutex);//��ȡ����Ȩ��ȷ��һ����������һ��������Դ��
            unsigned int idleCount = idle_count();
            unsigned int allCount = getCount();
            if ((idleCount + (max - allCount)) >= 1)
              {
                auto ipOnePool = Pool.find(sKey);
                if (ipOnePool != Pool.end())
                  {
                    //�ҵ��ж�Ӧ��Key�ĳ�
                    if (ipOnePool->second->idle_count() > 0)
                      {
                        re = ipOnePool->second->pop();
                        break;
                      }
                  }
                else
                  {
                    //û���ҵ�
                    Pool[sKey] = new gjm_m_respool<res>(max);//�½���Դ��
                  }

                if (max > allCount)//���������½���Դ
                  {
                    re = Pool[sKey]->pop();
					re->SetConnect(host, user, password, port);
                    break;
                  }
                else if (idleCount > 0)//���㹻�Ŀ�����Դ
                  {
                    int iMaxIdle = -1;
                    int iIdle = -1;
                    std::string sk;
                    for (auto &kv : Pool)
                      {
                        iIdle = kv.second->idle_count();
                        if (iIdle > iMaxIdle)
                          {
                            sk = kv.first;
                            iMaxIdle = iIdle;
                          }
                      }
                    if (iMaxIdle > 0)
                      {
                        Pool[sk]->del_one_idleres();
                        re = Pool[sKey]->pop();
						re->SetConnect(host, user, password, port);
                        break;
                      }
                  }
              }
            if (!boWait)
              {
                break;
              }
          }
          mysleep(1);
        }
      while (boWait);

      return re;
    }

    bool push(res *r)//�黹��Դ ���rû����useres�оͷ���false
    {
      bool boRE = false;
      if (r != nullptr)
        {
          std::lock_guard<std::mutex> lock(write_mutex);//�õ��ͷ���Դ��
          try
            {
              string sKey = r->getHost() + KeyFGF + r->getUser() + KeyFGF + r->getPassword() + KeyFGF + to_string(r->getPort());
              auto ipOnePool = Pool.find(sKey);
              if (ipOnePool != Pool.end())
                {
                  boRE = ipOnePool->second->push(r);
                }
            }
          catch (...) {}
        }
      return boRE;
    }
};
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
template <typename res>
class dbmysql_scoped_popres
{
  private:
    DBMysql_Pool<res> *m;
    res *r;
  protected:
  public:
    dbmysql_scoped_popres(DBMysql_Pool<res> &m_, const string &host,
                          const string &user, const string &password,
                          unsigned int port, bool boWait = true) : m(&m_)
    {
      r = m->pop(host, user, password, port, boWait);
    }
    ~dbmysql_scoped_popres(void)
    {
      m->push(r);
    }

    inline res *get(void)
    {
      return r;
    }
};
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
#endif