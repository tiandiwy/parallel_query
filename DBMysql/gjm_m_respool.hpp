//---------------------------------------------------------------------------
/*资源池类(非线程安全)只对空闲资源进行保留，对使用中的资源没有做管理 只做计数*/
//---------------------------------------------------------------------------
#ifndef gjm_m_respoolh
#define gjm_m_respoolh
//---------------------------------------------------------------------------
#include <string>
#include <queue>
#include <vector>
#include <unistd.h>
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
template <typename res>
class gjm_m_respool
{
  private:
    //mutex write_mutex;//修改锁
    //mutex pop_mutex;//停止获取资源锁

    queue<res *> idleres;//空闲资源
    deque<res *> useres;//使用中的资源


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
      res *re = nullptr;
      int iC = idleres.size();
      for (int i = 0; i < iC; i++)
        {
          re = idleres.front();
          idleres.pop();
          if (re != nullptr)
            {
              try
                {
                  delete re;
                  re = nullptr;
                }
              catch (...)
                {
                  re = nullptr;
                }
            }
        }
      iC = useres.size();
      for (int i = 0; i < iC; i++)
        {
          re = useres.front();
          useres.pop_front();
          if (re != nullptr)
            {
              try
                {
                  delete re;
                  re = nullptr;
                }
              catch (...)
                {
                  re = nullptr;
                }
            }
        }
    }

  protected:
  public:
    inline gjm_m_respool(void)
    {
      max = 0;
    }
    inline gjm_m_respool(unsigned int imax)
    {
      max = imax;
    }
    inline virtual ~gjm_m_respool(void)
    {
      clear();
    }

    inline unsigned int getCount(void)//当前资源数量
    {
      return idleres.size() + useres.size();
    };
    inline unsigned int idle_count(void)//当前空闲资源数量
    {
      return idleres.size();
    }
    inline unsigned int use_count(void)//当前使用中资源数量
    {
      return useres.size();
    }

    void setmax(unsigned int new_max)//设置最大资源数
    {
      //std::lock_guard<std::mutex> lock(write_mutex);//只需要锁一道就可以了，因为本函数在发现没有空闲资源只需退出即可，无需等待新的资源
      if (new_max == max)
        {
          return;
        }
      if (new_max > max)
        {
          max = new_max;
          return;
        }
      unsigned int is = idleres.size();
      if ((new_max < max) && (is > 0))//发现要减少最大资源数，并且有足够的空闲资源数，就开始清除掉多余的空闲资源
        {
          unsigned int iC = max - new_max;
          max = new_max;
          res *re = nullptr;
          int iCount = is > iC ? iC : is;
          for (int i = 0; i < iCount; i++)
            {
              re = idleres.front();
              idleres.pop();
              if (re != nullptr)
                {
                  try
                    {
                      delete re;
                      re = nullptr;
                    }
                  catch (...)
                    {
                      re = nullptr;
                    }
                }
            }
        }
    }
    inline unsigned int getmax(void)
    {
      return max; //获取最大资源数
    }

    bool empty(void)
    {
      max = 0 ? true : false;  //当前资源池是否为空
    }

    res *pop(void)//取出资源
    {
      res *re = nullptr;
      //std::lock_guard<std::mutex> lock_pop(pop_mutex);//先对pop锁住

      if (idleres.size() > 0)//有足够的空闲资源
        {
          re = idleres.front();
          idleres.pop();
          useres.push_back(re);
        }
      else if (max > getCount())//还可以再新建资源
        {
          re = new res;
          useres.push_back(re);
        }
      return re;
    }

	bool del_one_idleres(void)//删除一个空闲资源
    {
      if (idleres.size() > 0)//有足够的空闲资源
        {
          res *re = idleres.front();
          if (re!=nullptr)
            {
              try
                {
                  delete re;
                  re = nullptr;
                }
              catch (...) {}
            }
          idleres.pop();
          return true;
        }
      else
        {
          return false;
        }
    }

    bool push(res *r)//归还资源 如果r没有在useres中就返回false
    {
      bool boRE = false;
      //std::lock_guard<std::mutex> lock(write_mutex);//拿到释放资源锁
      //deque<res *>::iterator it;
      auto it = useres.begin();
      //deque<res *>::iterator it_end = useres.end();
      auto it_end = useres.end();
      for (; it != it_end; it++)//开始遍历r 是否在useres中
        {
          if ((*it) == r)
            {
              useres.erase(it);
              boRE = true;
              if (max >= getCount())//如果最大资源数大于等于现有资源数，那么保留当前资源
                {
                  idleres.push(r);//
                }
              else
                {
                  //如果最大资源数小于现有资源数，那么删除当前资源
                  try
                    {
                      delete r;
                    }
                  catch (...) {}
                }
              break;
            }
        }
      return boRE;
    }
};
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
//---------------------------------------------------------------------------
#endif
