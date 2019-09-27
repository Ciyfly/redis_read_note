#!/usr/bin/python
# coding=UTF-8
'''
@Author: recar
@Date: 2019-09-27 11:37:22
@LastEditTime: 2019-09-27 15:51:48
'''

from datetime import datetime
from threading import Thread
import redis
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

redis_server = "127.0.0.1"
redis_port = 6379

class Scheduler(object):
    # 基于redis实现一个定时调度功能

    def  __init__(self, schedule=False):
        self.is_schedule = schedule
        self.conn = self.get_connection()
        self.zset_delay_key = "zset_delay:"
        self.zset_schedule_key = "zset_schedule:"
        if self.is_schedule:
            self.schedule()

    def get_connection(self):
        connect = redis.Redis(host=redis_server, port=redis_port,db=0)
        return connect

    def add_task(self, task_id, delay):
        # 添加时间间隔
        self.conn.zadd(self.zset_delay_key, {task_id: delay})
        # 添加调度任务 这个会要先生成一个立即调度的 
        self.conn.zadd(self.zset_schedule_key, {task_id: time.time()})
    
    def schedule(self):
        while True:
            next = self.conn.zrange(self.zset_schedule_key, 0, 0, withscores=True)
            now = time.time()
            # print(next)
            if not next or next[0][1] > now:
                # print('next: {0} -> now: {1}'.format(next[0][1], now))
                time.sleep(0.5)
                continue
            task_id = next[0][0]
            # 获取时间间隔
            delay = self.conn.zscore(self.zset_delay_key, task_id)
            # 如果更新了时间间隔 为0 那么都删除掉
            if delay <=0:
                self.conn.zrem(self.zset_delay_key, task_id)
                self.conn.zrem(self.zset_schedule_key, task_id)
                continue
            print('exec task: {0} time: {1}'.format(task_id, now))
            # 否则更新调度任务
            self.conn.zrem(self.zset_schedule_key, task_id)
            self.conn.zadd(self.zset_schedule_key, {task_id: now+delay})
            
def main():
    try:
        # 启动一个定时调度监控
        def statr_schedule():
            schedule = Scheduler(schedule=True)
        t = Thread(target=statr_schedule, args=())
        t.start()
        print('start schedule')
        schedule = Scheduler()
        schedule.add_task(1, 5)
        print('add task: {0}'.format(1))
        schedule.add_task(2, 10)
        print('add task: {}'.format(2))
    except Exception as e:
        print(e)
        exit(1)

if __name__ == "__main__":
    main()
