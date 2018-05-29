# -*- coding: utf-8 -*-
"""
Created on Wed Mar 28 10:36:14 2018

@author: imuyang
"""
#coding:utf8

import time
from datetime import datetime,timedelta
import random
from utils import *
from multiprocessing.dummy import Pool as ThreadPool
from itertools import izip_longest

from model import Monitor_Post,Post,engine
from selenium.common.exceptions import NoSuchElementException

from sqlalchemy import *
from sqlalchemy.orm import *

DBSession = sessionmaker(bind=engine)
session = DBSession()


def read_monitors():
    uid_list = session.query(Monitor_Post.uid).all()
    uid_list = [res[0] for res in uid_list]
    session.commit()
    uid_list = list(set(uid_list))
    return uid_list

def read_monitors_manual():
    f = open("monitor_manual.txt","r")
    uid_list = f.readlines()
    f.close()
    return uid_list


def crawl_user_weibo_perWindow(username,password,window_size,uid_list):
    """
    目前机制是爬取以爬虫开始运行时间为结尾时间的一个时间窗口内的数据
    """
    
    #记录代码中使用的字符串和datetime数据结构之间的转换：
    #from datetime import datetime
    #a = datetime.now().strftime('%Y-%m-%d %H:%M:%S')获取当前时间,字符串形式
    #b = datetime.strptime(a,'%Y-%m-%d %H:%M:%S')字符串转换为datetime类型
    
    #window_size的单位为： 小时

    #获取时间窗口参数
    cur_time = datetime.now()
    start_day = str(cur_time.day)
    start_month = str(cur_time.month)
    end_day = start_day
    end_month = start_month
    if cur_time.hour + window_size > 24:
        inOneDay_flag = False
        return inOneDay_flag
    
    window_start_time = cur_time - timedelta(hours=window_size)
    #先模拟登录
    browser = LoginWeibo(username,password)
    url_base = "http://weibo.com/"
    url_list = [url_base + i for i in uid_list]

    for uid in uid_list:
        #基于expert_crawler.py更新而来，目前直接在url中提交检索时间
        cur_day = cur_time.strftime('%Y-%m-%d')
        search_url = "is_ori=1&is_forward=1&is_text=1&is_pic=1&is_video=1&is_music=1&is_article=1&key_word=&start_time="+cur_day+"&end_time="+cur_day+"&gid=&is_search=1&is_searchadv=1#_0"
        url = url_base + uid + "?" + search_url
        browser.get(url)
        print u"准备访问用户主页......",uid
        time.sleep(10)
        
        #爬取所有页面内微博
        next_page_xpath = "//a[@class='page next S_txt1 S_line1']"
        next_flag = True
        while next_flag: #让动态页面完全加载 for j in range(3):
            for i in range(3):
                js = "window.scrollTo(0,document.body.scrollHeight);"
                browser.execute_script(js)
                time.sleep(3)

            #爬取页面内所有微博文本
            feed_list_xpath = ".//div[@action-type='feed_list_item']"
            relative_weibo_content_xpath = ".//div[@class='WB_text W_f14']"
            relative_weibo_commentNum_xpath = ".//span[@node-type='comment_btn_text']/span/em[2]"
            relative_weibo_forwardNum_xpath = ".//span[@node-type='forward_btn_text']/span/em[2]"
            relative_weibo_likeNum_xpath = ".//a[@action-type='fl_like']/span/span/span/em[2]"
            relative_weibo_publishTime_xpath = ".//a[@node-type='feed_list_item_date']"
            feed_list = browser.find_elements_by_xpath(feed_list_xpath)
            if len(feed_list) == 0:
                sleep_time = random.randint(120,360)
                time.sleep(sleep_time)
            for feed in feed_list:
                in_last_window = False
                content = feed.find_element_by_xpath(relative_weibo_content_xpath).text
                comment_num = feed.find_element_by_xpath(relative_weibo_commentNum_xpath).text
                forward_num = feed.find_element_by_xpath(relative_weibo_forwardNum_xpath).text
                like_num = feed.find_element_by_xpath(relative_weibo_likeNum_xpath).text
                weibo_publish_time = feed.find_element_by_xpath(relative_weibo_publishTime_xpath).text
                print "时间字段为：",weibo_publish_time
                if u"分钟前" in weibo_publish_time:
                    weibo_publish_time = filter(str.isdigit,str(weibo_publish_time))
                    in_last_window = True
                elif u"秒前" in weibo_publish_time:
                    in_last_window = True
                elif u"今天" in weibo_publish_time:
                    tem = weibo_publish_time.split(u"今天")[1]
                    that_hour = int(tem.split(":")[0])
                    that_minute = int(tem.split(":")[1])
                    that_date = datetime(2018,end_month,end_day,that_hour,that_minute)
                    time_delta_byHour = (cur_time - that_date).hours 
                    if time_delta_byHour < window_size:
                        in_last_window = True
                    else:
                        in_last_window = False
                else:
                    in_last_window = False
                if in_last_window == False:
                    continue
                mid = feed.get_attribute("mid")
                time_window = window_start_time.strftime('%Y-%m-%d %H:%M:%S') + " - " + cur_time.strftime('%Y-%m-%d %H:%M:%S')
                weibo_tuple = (mid,uid,forward_num,like_num,comment_num,time_window,content)
                print "weibo_tuple: ",weibo_tuple
                try:
                    write_post_perWindow().send(weibo_tuple)
                except StopIteration:
                    continue
                    

            if has_element_byXpath(browser,next_page_xpath):
                next_page_elem = browser.find_element_by_xpath(next_page_xpath)
                browser.execute_script("arguments[0].click();",next_page_elem)
                time.sleep(3)
            else:
                next_flag = False
    endtime = datetime.now()
    elapsed_time = (endtime - cur_time).seconds
    print "time used for around crawl is: ",elapsed_time
    
    
    
def crawl_user_weibo_perWindow_ForParallel(task_list):
    """
    爬取任意时间窗口内的数据，基于crawl_user_weibo_perWindow()更改而来
    """   
    #记录代码中使用的字符串和datetime数据结构之间的转换：
    #from datetime import datetime
    #a = datetime.now().strftime('%Y-%m-%d %H:%M:%S')获取当前时间,字符串形式
    #b = datetime.strptime(a,'%Y-%m-%d %H:%M:%S')字符串转换为datetime类型   
    #window_size的单位为： 小时 
    username,password = task_list[1]
    uid_list = task_list[0]
    #先模拟登录
    browser = LoginWeibo(username,password)
    url_base = "http://weibo.com/"
    round_count = 0
    while 1:
        #获取时间窗口参数
        #window_end_time,window_size = task_list[2:]
        print "请注意：本线程将要开始第 " + str(round_count) + "轮爬取...............\n" * 6
        window_end_time = datetime.now()
        window_size = 2
        window_start_time = window_end_time - timedelta(hours=window_size)
        
        for uid in uid_list:
            #基于expert_crawler.py更新而来，目前直接在url中提交检索时间
            start_ymd = window_start_time.strftime('%Y-%m-%d')
            end_ymd = window_end_time.strftime('%Y-%m-%d')
            search_url = "is_ori=1&is_forward=1&is_text=1&is_pic=1&is_video=1&is_music=1&is_article=1&key_word=&start_time="+start_ymd+"&end_time="+end_ymd+"&gid=&is_search=1&is_searchadv=1#_0"
            if uid == None:
                continue
            url = url_base + uid + "?" + search_url
            browser.get(url)
            print u"准备访问用户主页......",uid
            time.sleep(10)
            #处理提示“访问太频繁，请稍候再试”
            none_xpath_3 = "//a[contains(text(),'查看全部微博')]"
            if has_element_byXpath(browser,none_xpath_3):
                print "该用户在待爬时间窗口内没有发表微博，跳过该用户"
                continue
            #爬取所有页面内微博
            next_page_xpath = "//a[@class='page next S_txt1 S_line1']"
            next_flag = True
            page_count = 0
            next_uid = False
            while next_flag and page_count < 3: #让动态页面完全加载 for j in range(3):
                for i in range(3):
                    js = "window.scrollTo(0,document.body.scrollHeight);"
                    browser.execute_script(js)
                    time.sleep(3)
                #爬取页面内所有微博文本
                feed_list_xpath = ".//div[@action-type='feed_list_item']"
                relative_weibo_content_xpath = ".//div[@class='WB_text W_f14']"
                relative_weibo_commentNum_xpath = ".//span[@node-type='comment_btn_text']/span/em[2]"
                relative_weibo_forwardNum_xpath = ".//span[@node-type='forward_btn_text']/span/em[2]"
                relative_weibo_likeNum_xpath = ".//a[@action-type='fl_like']/span/span/span/em[2]"
                relative_weibo_publishTime_xpath = ".//a[@node-type='feed_list_item_date']"
                feed_list = browser.find_elements_by_xpath(feed_list_xpath)
                while len(feed_list) == 0:
                    sleep_time = random.randint(120,360)
                    time.sleep(sleep_time)
                    browser.refresh()
                    time.sleep(3)
                    if has_element_byXpath(browser,none_xpath_3):
                        print "该用户在待爬时间窗口内没有发表微博，跳过该用户"
                        next_uid = True
                        break
                    for i in range(3):
                        js = "window.scrollTo(0,document.body.scrollHeight);"
                        browser.execute_script(js)
                        time.sleep(3)
                    feed_list = browser.find_elements_by_xpath(feed_list_xpath)
                if next_uid == True:
                    break
                for feed in feed_list:
                    content = feed.find_element_by_xpath(relative_weibo_content_xpath).text
                    comment_num = feed.find_element_by_xpath(relative_weibo_commentNum_xpath).text
                    forward_num = feed.find_element_by_xpath(relative_weibo_forwardNum_xpath).text
                    like_num = feed.find_element_by_xpath(relative_weibo_likeNum_xpath).text
                    weibo_publish_time = feed.find_element_by_xpath(relative_weibo_publishTime_xpath).text.encode("utf-8")
                    if "评论" in str(comment_num.encode("utf-8")):
                        comment_num = "0"
                    if "转发" in str(forward_num.encode("utf-8")):
                        forward_num = "0"
                    if "赞" in str(like_num.encode("utf-8")):
                        like_num = "0"
                    mid = feed.get_attribute("mid")
                    datetime_now = datetime.now()
                    weibo_publish_time = str(weibo_publish_time)
                    print weibo_publish_time
                    #print type(weibo_publish_time)
                    if "分钟前" in weibo_publish_time:
                        minutes = filter(str.isdigit,str(weibo_publish_time))
                        that_date = datetime_now - timedelta(minutes=int(minutes))
                    elif "秒前" in weibo_publish_time:
                        seconds = filter(str.isdigit,str(weibo_publish_time))
                        that_date = datetime_now - timedelta(seconds=int(seconds))
                    elif "今天" in weibo_publish_time:
                        tem = weibo_publish_time.split("今天")[1]
                        that_hour = int(tem.split(":")[0])
                        that_minute = int(tem.split(":")[1])
                        that_date = datetime(2018,datetime_now.month,datetime_now.day,that_hour,that_minute)
                    elif "月" in weibo_publish_time:
                        that_month = int(weibo_publish_time.split("月")[0])
                        tem = weibo_publish_time.split("月")[1]
                        that_day = int(tem.split("日")[0])
                        that_hm = tem.split("日")[1].strip()
                        that_hour,that_minute = that_hm.split(":")
                        that_hour = int(that_hour)
                        that_minute = int(that_minute)
                        that_date = datetime(2018,that_month,that_day,that_hour,that_minute)
                    else:
                        print "页面时间字段出错！！！"
                    if not (that_date <= window_end_time and that_date >= window_start_time):
                        print "该微博不在待爬时间窗口内，跳过该微博"
                        continue
                        
                    time_window = window_start_time.strftime('%Y-%m-%d %H:%M:%S') + " - " + window_end_time.strftime('%Y-%m-%d %H:%M:%S')
                    weibo_tuple = (mid,uid,forward_num,like_num,comment_num,time_window,content)
                    print "weibo_tuple: ",weibo_tuple
                    try:
                        write_post_perWindow().send(weibo_tuple)
                    except StopIteration:
                        continue              
                if has_element_byXpath(browser,next_page_xpath) and page_count < 3:
                    next_page_elem = browser.find_element_by_xpath(next_page_xpath)
                    browser.execute_script("arguments[0].click();",next_page_elem)
                    page_count += 1
                    time.sleep(3)
                else:
                    next_flag = False
        round_count += 1
    browser.quit()
    
def single_main():
    uid_list = read_monitors()
    loop_num = 10
    for i in range(loop_num):
        crawl_user_weibo_perWindow("XXXXXX","XXXXXX",2,uid_list)
        
def parallel_main():
    uid_list = read_monitors()
    pool = ThreadPool(2)
    chunk_list = lambda a_list, n: izip_longest(*[iter(a_list)]*n)
    cut_list = list(chunk_list(uid_list, 280))
    account_list = [["XXXXXX","XXXXXX"],["XXXXXX","XXXXXX"]] * 500
    task_list = [[cut_list[i],account_list[i]] for i in range(len(cut_list))]
    pool.map(crawl_user_weibo_perWindow_ForParallel,task_list)
    
    #测试单线程
    #crawl_user_weibo_perWindow_ForParallel(task_list[0])
    
    

    
    
if __name__ == "__main__":
    #single_main()
    while 1:
        parallel_main()
        print "all round finished"
        
 
    