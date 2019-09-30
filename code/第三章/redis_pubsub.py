#!/usr/bin/python
# coding=UTF-8
'''
@Author: recar
@Date: 2019-09-30 17:03:47
@LastEditTime: 2019-09-30 17:17:25
'''

import time
import threading
import redis

conn = redis.Redis(host="127.0.0.1", port=6379)

#  创建队列
def publisher(n):
    time.sleep(1)
    for i in xrange(n):
        # 发消息
        print('send message: {0}'.format(i))
        conn.publish('channel', i)
        time.sleep(2)
    print('send over')
# 打印消息 
def run_pubsub():
    threading.Thread(target=publisher, args=(5, )).start()
    pubsub = conn.pubsub()
    pubsub.subscribe('channel')
    count =0
    for item in pubsub.listen():
        print(item)
        count +=1
        if count == 4:
            # 退出订阅
            pubsub.unsubscribe()
        if count == 5:
            break

run_pubsub()
