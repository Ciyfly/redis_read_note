#!/usr/bin/python
# coding=UTF-8
'''
@Author: Recar
@Date: 2019-11-08 11:07:55
@LastEditTime: 2019-11-08 12:27:06
'''
# redis watch 用于秒杀超卖问题的解决
from datetime import datetime
from threading import Thread
from threading import Lock
import redis
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
lock = Lock()

class WatchDemo(object):  

    def  __init__(self):
        self.conn = self.get_connection()
        self.goods_key = 'goods'
        self.set_init_goods()

    def get_connection(self):
        connect = redis.Redis(host='127.0.0.1', port=6379 ,db=0)
        return connect
    
    def set_init_goods(self):
        self.conn.set(self.goods_key, 5)
    
    def consumer(self, name):
        flag = False
        while True:
            try:
                pipe = self.conn.pipeline()
                pipe.watch(self.goods_key)
                goods_count = int(pipe.get(self.goods_key))
                if goods_count>0:
                    pipe.multi() 
                    pipe.set(self.goods_key, goods_count-1)
                    pipe.execute()          # 执行事务
                    flag = True
                break
            except redis.exceptions.WatchError:
                lock.acquire()
                print('consumer {0} retry...'.format(name))
                lock.release()
                continue
            except Exception as e:
                print(e)
                break
        if flag:
            lock.acquire()
            print('consumer {0} success  : {1}'.format(name, goods_count))
            lock.release()
        else:
            lock.acquire()
            print('consumer {0} faild'.format(name))
            lock.release()
        
def main():
    wd = WatchDemo()
    threads = list()
    for i in range(0,10):
        t = Thread(target=wd.consumer, args=(i,))
        threads.append(t)
    for t in threads:
        t.start()

if __name__ == "__main__":
    main()

"""
consumer 0 success  : 5
consumer 1 success  : 4
consumer 2 success  : 3
consumer 3 retry...
consumer 4 success  : 2
consumer 5 success  : 1
consumer 6 faild
consumer 3 faild
consumer 7 faild
consumer 8 faild
consumer 9 faild
"""