//---------------------------------------------------------------------------
/*资源池类 只对空闲资源进行保留，对使用中的资源没有做管理 只做计数         */
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

    mutex write_mutex;//修改锁
    mutex pop_mutex;//停止获取资源锁

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
    void clear(void)//清空资源池
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

    inline unsigned int getCount(void)//当前资源数量
    {
      unsigned int count = 0;
      for (auto &kv : Pool)
        {
          count += kv.second->getCount();
        }
      return count;
    }

    inline unsigned int idle_count(void)//当前空闲资源数量
    {
      unsigned int idleres = 0;
      for (auto &kv : Pool)
        {
          idleres += kv.second->idle_count();
        }
      return idleres;
    }

    void clear_idle(void)//清空空闲资源
    {
      max = 0;
      for (auto &kv : Pool)
        {
          if (kv.second != nullptr)
            {
              kv.second->clear_idle();
            }
        }
      Pool.clear();
    }

    inline unsigned int use_count(void)//当前使用中资源数量
    {
      unsigned int useres = 0;
      for (auto &kv : Pool)
        {
          useres += kv.second->use_count();
        }
      return useres;
    }

    void setmax(unsigned int new_max)//设置最大资源数
    {
      std::lock_guard<std::mutex> lock(write_mutex);//只需要锁一道就可以了，因为本函数在发现没有空闲资源只需退出即可，无需等待新的资源

      for (auto &kv : Pool)
        {
          kv.second->setmax(new_max);//让每个子资源池与父资源池总容量相同
        }
      max = new_max;
    }
    inline unsigned int getmax(void)
    {
      return max; //获取最大资源数
    }

    bool empty(void)
    {
      max = 0 ? true : false;  //当前资源池是否为空
    }

    res *pop(const string &host, const string &user, const string &password, unsigned int port, const string &ConnectedRunSQL = "", bool boWait = true) //取出资源
    {
      res *re = nullptr;
      if (max <= 0)
        {
          return nullptr;
        }
      std::lock_guard<std::mutex> lock_pop(pop_mutex);//先对pop锁住
      string sKey = host + KeyFGF + user + KeyFGF + password + KeyFGF + to_string(port);

      do
        {
          {
            //此作用区保证write_mutex会在mysleep函数之前自动解开
            std::lock_guard<std::mutex> lock_write(write_mutex);//获取控制权，确保一定会有至少一个可用资源！
            unsigned int idleCount = idle_count();
            unsigned int allCount = getCount();
            if ((idleCount + (max - allCount)) >= 1)
              {
                auto ipOnePool = Pool.find(sKey);
                if (ipOnePool != Pool.end())
                  {
                    //找到有对应的Key的池
                    if (ipOnePool->second->idle_count() > 0)
                      {
                        re = ipOnePool->second->pop();
                        break;
                      }
                  }
                else
                  {
                    //没有找到
                    Pool[sKey] = new gjm_m_respool<res>(max);//新建资源池
                  }

                if (max > allCount)//还可以再新建资源
                  {
                    re = Pool[sKey]->pop();
                    re->SetConnect(host, user, password, port, ConnectedRunSQL);
                    break;
                  }
                else if (idleCount > 0)//有足够的空闲资源
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
                        re->SetConnect(host, user, password, port, ConnectedRunSQL);
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

    bool push(res *r)//归还资源 如果r没有在useres中就返回false
    {
      bool boRE = false;
      if (r != nullptr)
        {
          std::lock_guard<std::mutex> lock(write_mutex);//拿到释放资源锁
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
                          unsigned int port, const string &ConnectedRunSQL = "", bool boWait = true) : m(&m_)
    {
      r = m->pop(host, user, password, port, ConnectedRunSQL, boWait);
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