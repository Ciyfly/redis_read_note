#!/usr/bin/python
# coding=UTF-8
'''
@Author: recar
@Date: 2019-09-24 19:28:11
@LastEditTime: 2019-09-25 18:05:38
'''
from datetime import datetime
import redis
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

redis_server = "127.0.0.1"
redis_port = 6379

class Article(object):
    # 实现一个对于文章点赞点踩的功能  

    def  __init__(self):
        self.conn = self.get_connection()
        self.zset_article_by_time_key = "article_by_time"
        self.zset_article_by_like_key = "article_by_like"
        self.hash_article_prefix = "hash_info_article:"
        self.set_article_prefix = "set_author_like_article:"
        # 分值常量 一天s数/ 需要多少票认为是好的文章
        self.constantvalue = 86400 / 10

    def get_connection(self):
        connect = redis.Redis(host=redis_server, port=redis_port,db=0)
        return connect

    def add_article(self, article_id, title, author,connect):
        # 添加文章
        hash_key = self.hash_article_prefix + str(article_id)
        self.conn.hset(hash_key, "title", title)
        self.conn.hset(hash_key, "author", author)
        self.conn.hset(hash_key, "connect", connect)
        create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        time_tuple = time.strptime(create_time, '%Y-%m-%d %H:%M:%S')
        timeStamp = time.mktime(time_tuple)
        self.conn.hset(hash_key, "create_time", create_time)
        # 这里要添加一个文章发布的基于时间的有序集合
        self.conn.zadd(self.zset_article_by_time_key, {article_id: timeStamp})
        # 创建一个对文章点赞的集合 防止一个人对文章多次点赞
        set_key = self.set_article_prefix + str(article_id)
        self.conn.sadd(set_key, author)
        # 创建一个对文章点赞数的有序集合 用于排序展示  初始都是0
        self.conn.zadd(self.zset_article_by_like_key, {article_id: 0})
        print("author: {0} add article: {1}".format(author, article_id))

    def like_article(self, article_id, author):
        # 文章被点赞 先判断这个用户是否已经对文章点过赞了 
        set_key = self.set_article_prefix + str(article_id)
        is_exists = self.conn.sismember(set_key, author)
        if is_exists:
            return 
        else:
            # 将用户添加到集合并 
            self.conn.sadd(set_key, author)
            #对文章有序集合like自增分值 
            self.conn.zincrby(self.zset_article_by_like_key, self.constantvalue, article_id)
            print("author: {0} like article {1}".format(author, article_id))

    def get_article_by_like_time(self):
        # 获取时间加文章点赞作为分值的排序
        self.conn.zinterstore("zset_like_time", 
            (self.zset_article_by_time_key, self.zset_article_by_like_key),
            aggregate="SUM"
        )
        print("article:")
        print("================================")
        print(self.conn.zrange("zset_like_time", 0, -1, desc=True))
        print("================================")

    def clear(self):
        for key in self.conn.keys():
            self.conn.delete(key)
        print("flushall")

def main():
    article  = Article()
    # 添加三篇文章
    article.add_article(1,"one",1,"111111111")
    article.add_article(2,"two",2,"222222222")
    article.add_article(3,"three",3,"3333333")

    # 先输出文章
    article.get_article_by_like_time()

    # 点赞
    article.like_article(2,4)
    article.like_article(2,5)
    article.like_article(2,6)

    article.like_article(1,6)
    article.like_article(1,7)

    article.like_article(3,8)
    # 输出文章排序
    article.get_article_by_like_time()

    article.clear()

if __name__ == "__main__":
    main()