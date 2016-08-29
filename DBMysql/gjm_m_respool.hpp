//---------------------------------------------------------------------------
/*��Դ����(���̰߳�ȫ)ֻ�Կ�����Դ���б�������ʹ���е���Դû�������� ֻ������*/
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
    //mutex write_mutex;//�޸���
    //mutex pop_mutex;//ֹͣ��ȡ��Դ��

    queue<res *> idleres;//������Դ
    deque<res *> useres;//ʹ���е���Դ


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

    inline unsigned int getCount(void)//��ǰ��Դ����
    {
      return idleres.size() + useres.size();
    };
    inline unsigned int idle_count(void)//��ǰ������Դ����
    {
      return idleres.size();
    }
    inline unsigned int use_count(void)//��ǰʹ������Դ����
    {
      return useres.size();
    }

    void setmax(unsigned int new_max)//���������Դ��
    {
      //std::lock_guard<std::mutex> lock(write_mutex);//ֻ��Ҫ��һ���Ϳ����ˣ���Ϊ�������ڷ���û�п�����Դֻ���˳����ɣ�����ȴ��µ���Դ
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
      if ((new_max < max) && (is > 0))//����Ҫ���������Դ�����������㹻�Ŀ�����Դ�����Ϳ�ʼ���������Ŀ�����Դ
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
      return max; //��ȡ�����Դ��
    }

    bool empty(void)
    {
      max = 0 ? true : false;  //��ǰ��Դ���Ƿ�Ϊ��
    }

    res *pop(void)//ȡ����Դ
    {
      res *re = nullptr;
      //std::lock_guard<std::mutex> lock_pop(pop_mutex);//�ȶ�pop��ס

      if (idleres.size() > 0)//���㹻�Ŀ�����Դ
        {
          re = idleres.front();
          idleres.pop();
          useres.push_back(re);
        }
      else if (max > getCount())//���������½���Դ
        {
          re = new res;
          useres.push_back(re);
        }
      return re;
    }

	bool del_one_idleres(void)//ɾ��һ��������Դ
    {
      if (idleres.size() > 0)//���㹻�Ŀ�����Դ
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

    bool push(res *r)//�黹��Դ ���rû����useres�оͷ���false
    {
      bool boRE = false;
      //std::lock_guard<std::mutex> lock(write_mutex);//�õ��ͷ���Դ��
      //deque<res *>::iterator it;
      auto it = useres.begin();
      //deque<res *>::iterator it_end = useres.end();
      auto it_end = useres.end();
      for (; it != it_end; it++)//��ʼ����r �Ƿ���useres��
        {
          if ((*it) == r)
            {
              useres.erase(it);
              boRE = true;
              if (max >= getCount())//��������Դ�����ڵ���������Դ������ô������ǰ��Դ
                {
                  idleres.push(r);//
                }
              else
                {
                  //��������Դ��С��������Դ������ôɾ����ǰ��Դ
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
