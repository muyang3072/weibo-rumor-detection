# -*- coding: utf-8 -*-
"""
Created on Tue Mar 27 20:32:12 2018

@author: imuyang
"""
import time
import random
from utils import *
from multiprocessing.dummy import Pool as ThreadPool
from itertools import izip_longest

from model import Monitor_Post,engine
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime,timedelta

from sqlalchemy import *
from sqlalchemy.orm import *

DBSession = sessionmaker(bind=engine)
session = DBSession()


def crawl_monitor_uidAndName_single_ForParallel():
    #username,password = task_list[1]
    username = "XXXXXX" #此处填写自己的账号和密码
    password = "XXXXXX"
    domain_list = [
            "2",
            "4",#时评
            "14",
            "16",
            "17",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "28",
            "29",
            "30",
            "31",
            "32",
            "34",
            "35",
            "39",
            "40",
            "41",
            "42",
            "44",
            "45",
            "46",
            "47"
            ]
    b = LoginWeibo(username,password)
    
    time.sleep(5)
    domain_more_xpath = "//a[@class='item_link S_txt1' and @node-type='fold_btn']"
    elems_xpath = "//li[@class='follow_item S_line2']"
    for domain in domain_list:
        b.get("https://d.weibo.com/1087030002_2975_5001_0#")
        time.sleep(6)
        b.find_element_by_xpath(domain_more_xpath).click()
        time.sleep(6)
        domain_xpath = "//div[@class='subitem_box S_line1']/ul[@class='ul_item clearfix']/li["+domain+"]"
        b.find_element_by_xpath(domain_xpath).click()
        time.sleep(10)
        next_page_xpath = "//a[@class='page next S_txt1 S_line1']"
        next_flag = True
        count = 0
        while next_flag and count < 10: #让动态页面完全加载 for j in range(3):
            elem_list = b.find_elements_by_xpath(elems_xpath)
            for elem in elem_list:
                sub_elem  = elem.find_element_by_xpath("./dl/dd[1]/div[1]/a")
                uid = sub_elem.get_attribute("usercard")
                uid = str(uid.encode("utf-8"))
                uid = uid.split("&")[0].split("=")[1]
                name = sub_elem.get_attribute("title")
                print "uid: ",uid
                print "name: ",name
                try:
                    write_monitor_uidAndName().send((uid,name))
                except StopIteration:
                    continue
            if has_element_byXpath(b,next_page_xpath) and count < 10:
                next_page_elem = b.find_element_by_xpath(next_page_xpath)
                b.execute_script("arguments[0].click();",next_page_elem)
                count +=1
                time.sleep(5)
            else:
                next_flag = False
   
    
def crawl_monitor_uidAndName_Parallel():
    uid_list = session.query(Monitor_Post.uid).filter(Monitor_Post.content == None).all()
    uid_list = [res[0] for res in uid_list]
    session.commit()
    num_list = [
            "2",
            "4",#时评
            "14",
            "16",
            "17",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "28",
            "29",
            "30",
            "31",
            "32",
            "34",
            "35",
            "39",
            "40",
            "41",
            "42",
            "44",
            "45",
            "46",
            "47"
            ]
    chunk_list = lambda a_list, n: izip_longest(*[iter(a_list)]*n)
    cut_list = list(chunk_list(num_list, 4))
    account_list = (["XXXXXX","XXXXXX"],["XXXXXX","XXXXXX"]) * 10
    task_list = [[cut_list[i],account_list[i]] for i in range(len(cut_list))]
    pool = ThreadPool(2)
    pool.map(crawl_monitor_uidAndName_single_ForParallel,task_list)
    
    
def crawl_expert_recent10days_single_forParallel(task_list):
    
    uid_list = task_list[0]
    print "get task numbers: ",len(uid_list)
    username,password = task_list[1]
    b = LoginWeibo(username,password)
    start_month = "3"
    start_day = "17"
    end_month = "3"
    end_day = "27"
    empty_xpath = "//div[@class='WB_empty']"
    content = ""
    exit_flag = False
    for uid in uid_list:
        try:
            url = "https://weibo.com/"+str(uid)#+"?refer_flag=1087030101_417"
            print url
            sleep_time = random.randint(5,10)
            time.sleep(sleep_time)
            b.get(url)
            print u"准备访问用户主页......",uid
            time.sleep(10)
            try:
                follower_num_xpath = "//td[@class='S_line1'][1]//strong"
                followee_num_xpath = "//td[@class='S_line1'][2]//strong"
                post_num_xpath = "//td[@class='S_line1'][3]//strong"
                is_verified_xpath = "//div[@class='verify_area W_tog_hover S_line2']"
                follower_num = b.find_element_by_xpath(follower_num_xpath).text
                followee_num = b.find_element_by_xpath(followee_num_xpath).text
                post_num = b.find_element_by_xpath(post_num_xpath).text
                is_verified = "False"
                if has_element_byXpath(b,is_verified_xpath):
                    is_verified = "True"
                home_xpath = "//table[@class='tb_tab']/tbody/tr[1]/td[1]"
                b.find_element_by_xpath(home_xpath).click()
                time.sleep(5)
                elem_all_xpath = "//li[@class='tab_li tab_li_first']"
                b.find_element_by_xpath(elem_all_xpath).click()
                time.sleep(3)
                arrow_down_xpath = "//span[@class='WB_search_s']/form/span/a[2]"
                arrow_down_elem = b.find_element_by_xpath(arrow_down_xpath)
                b.execute_script("arguments[0].click();",arrow_down_elem)
            except Exception,e:
                print u"打开全部微博页面选项异常: ",e
    
            #把内容限定在指定范围内
            start_time_xpath = "//*[@id='start_time']"
            start_month_xpath = "//div[@class='pc_caldr W_layer']/div[1]/select[1]"
            start_spec_month_xpath = "//div[@class='pc_caldr W_layer']/div[1]/select[1]/option["+start_month+"]"
            which_day_start_xpath = "//div[@class='pc_caldr W_layer']//a[@day="+start_day+"]"
            end_time_xpath = "//*[@id='end_time']"
            end_month_xpath = "//div[@class='pc_caldr W_layer']/div/select[1]"
            end_spec_month_xpath = "//div[@class='pc_caldr W_layer']/div/select[1]/option["+end_month+"]"
            which_day_end_xpath = "//div[@class='pc_caldr W_layer']//a[@day="+end_day+"]"
            search_button_xpath = "//a[@action-type='search_button']"
            time.sleep(3)
            b.find_element_by_xpath(start_time_xpath).click()
            b.find_element_by_xpath(start_month_xpath).click()
            b.find_element_by_xpath(start_spec_month_xpath).click()
            b.find_element_by_xpath(which_day_start_xpath).click()
            time.sleep(3)
            b.find_element_by_xpath(end_time_xpath).click()
            b.find_element_by_xpath(end_month_xpath).click()
            b.find_element_by_xpath(end_spec_month_xpath).click()
            b.find_element_by_xpath(which_day_end_xpath).click()
            b.find_element_by_xpath(search_button_xpath).click()
#            if has_element_byXpath(b,empty_xpath):
#                b.quit()
            time.sleep(3)
    
            #爬取所有页面内微博
            next_page_xpath = "//a[@class='page next S_txt1 S_line1']"
            next_flag = True
            count = 0
            while next_flag and count < 3: #让动态页面完全加载 for j in range(3):
                for i in range(3):
                    js = "window.scrollTo(0,document.body.scrollHeight);"
                    b.execute_script(js)
                    time.sleep(3)
    
                #爬取页面内所有微博文本
                feed_list_xpath = ".//div[@action-type='feed_list_item']"
                relative_weibo_content_xpath = ".//div[@class='WB_text W_f14']"
                feed_list = b.find_elements_by_xpath(feed_list_xpath)
                for feed in feed_list:
                    weibo_content = feed.find_element_by_xpath(relative_weibo_content_xpath).text
                    print "weibo_content: ",weibo_content

                    if weibo_content == "转发微博":
                        weibo_content = ""
                    content += weibo_content + "\n"

                #如果还有下一页，就更新翻页标志
                if has_element_byXpath(b,next_page_xpath) and count < 3:
                    next_page_elem = b.find_element_by_xpath(next_page_xpath)
                    b.execute_script("arguments[0].click();",next_page_elem)
                    count += 1
                    time.sleep(3)
                else:
                    next_flag = False
            if content != "":
                try:
                    write_monitor_content().send((uid,follower_num,followee_num,post_num,is_verified,content))
                except StopIteration:
                    continue
            else:
                sleep_time = random.randint(120,360)
                time.sleep(sleep_time)
        except:
            try:
                if content != "":
                    write_monitor_content().send((uid,follower_num,followee_num,post_num,is_verified,content))
                else:
                    continue
            except StopIteration:
                continue
    b.quit()


def crawl_expert_recent10days_single_forParallel_improved(task_list):
    username,password = task_list[1]
    #username,password = task_list
    uid_list = task_list[0]
    cur_time = datetime.now()

    #先模拟登录
    browser = LoginWeibo(username,password)
    url_base = "http://weibo.com/"
    none_xpath_3 = "//a[contains(text(),'查看全部微博')]"#一种被ban的迹象

    for uid in uid_list:
        #基于expert_crawler.py更新而来，目前直接在url中提交检索时间
        sleep_time = random.randint(30,60)
        time.sleep(sleep_time)
        start_time = "2018-03-17"
        end_time = "2018-03-27"
        search_url = "is_ori=1&is_forward=1&is_text=1&is_pic=1&is_video=1&is_music=1&is_article=1&key_word=&start_time="+start_time+"&end_time="+end_time+"&gid=&is_search=1&is_searchadv=1#_0"
        url = url_base + uid + "?" + search_url
        browser.get(url)
        print u"准备访问用户主页......",uid
        time.sleep(10)
        if has_element_byXpath(browser,none_xpath_3):
            print "该用户在待爬时间窗口内没有发表微博，跳过该用户"
            continue
        follower_num_xpath = "//td[@class='S_line1'][1]//strong"
        followee_num_xpath = "//td[@class='S_line1'][2]//strong"
        post_num_xpath = "//td[@class='S_line1'][3]//strong"
        is_verified_xpath = "//div[@class='verify_area W_tog_hover S_line2']"
        follower_num = browser.find_element_by_xpath(follower_num_xpath).text
        followee_num = browser.find_element_by_xpath(followee_num_xpath).text
        post_num = browser.find_element_by_xpath(post_num_xpath).text
        is_verified = "False"
        if has_element_byXpath(browser,is_verified_xpath):
            is_verified = "True"
        try:
            urepi_tup = (uid,follower_num,followee_num,post_num,is_verified)
            write_monitor_repi().send(urepi_tup)
        except StopIteration:
            print "完成monitor repi的写入"
        
        #爬取所有页面内微博
        next_page_xpath = "//a[@class='page next S_txt1 S_line1']"
        next_flag = True
        count = 0
        while next_flag and count < 5: #让动态页面完全加载 for j in range(3):
            for i in range(3):
                js = "window.scrollTo(0,document.body.scrollHeight);"
                browser.execute_script(js)
                time.sleep(3)

            #爬取页面内所有微博文本
            feed_list_xpath = ".//div[@action-type='feed_list_item']"
            relative_weibo_content_xpath = ".//div[@class='WB_text W_f14']"
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
            for feed in feed_list:
                content = feed.find_element_by_xpath(relative_weibo_content_xpath).text
                weibo_tuple = (uid,content)
                print "weibo_tuple: ",weibo_tuple
                try:
                    write_monitor_content().send(weibo_tuple)
                except StopIteration:
                    continue

            if has_element_byXpath(browser,next_page_xpath) and count < 5:
                next_page_elem = browser.find_element_by_xpath(next_page_xpath)
                browser.execute_script("arguments[0].click();",next_page_elem)
                count += 1
                time.sleep(3)
            else:
                next_flag = False
    endtime = datetime.now()
    elapsed_time = (endtime - cur_time).seconds
    print "time used for around crawl is: ",elapsed_time
    
    
def crawl_expert_recent10days_parallel():
    uid_list = session.query(Monitor_Post.uid).filter(Monitor_Post.content == None).all()
    uid_list = [res[0] for res in uid_list]
    session.commit()
    l = len(uid_list)
    chunk_list = lambda a_list, n: izip_longest(*[iter(a_list)]*n)
    cut_list = list(chunk_list(uid_list, 500))
    account_list = [["XXXXXX","XXXXXX"],["XXXXXX","XXXXXX"]] * 500#此处填写两组账号密码，以支持并发度为2的并发爬取
    task_list = [[cut_list[i],account_list[j]] for i,j in zip(range(len(cut_list)),range(len(account_list)))]
    pool = ThreadPool(2)
    pool.map(crawl_expert_recent10days_single_forParallel_improved,task_list)


def crawl_user_repi(uid_list):
    b = LoginWeibo()
    for uid in uid_list:
        existing_uids = session.query(Monitor_Post.uid).filter_by(post_number = None).all()
        existing_uids = [str(res[0].encode("utf-8")) for res in existing_uids]
        session.commit()
        if uid in existing_uids:
            print "已经爬取，跳过"
            continue
        url = "https://weibo.com/"+str(uid)#+"?refer_flag=1087030101_417"
        print url
        b.get(url)
        print u"准备访问用户主页......",uid
        time.sleep(10)
        follower_num_xpath = "//td[@class='S_line1'][1]//strong"
        followee_num_xpath = "//td[@class='S_line1'][2]//strong"
        post_num_xpath = "//td[@class='S_line1'][3]//strong"
        is_verified_xpath = "//div[@class='verify_area W_tog_hover S_line2']"
        follower_num = b.find_element_by_xpath(follower_num_xpath).text
        followee_num = b.find_element_by_xpath(followee_num_xpath).text
        post_num = b.find_element_by_xpath(post_num_xpath).text
        is_verified = "False"
        if has_element_byXpath(b,is_verified_xpath):
            is_verified = "True"
        #写入数据库
        try:
            urepi_tup = (uid,follower_num,followee_num,post_num,is_verified)
            write_monitor_repi().send(urepi_tup)
        except StopIteration:
            continue
    b.quit()
    


#def refine_uid():
#    """
#    为了解决第一次爬取monitor时，uid字段不规范的问题
#    """
#    monitors = session.query(Monitor_Post).all()
#    session.commit()
#    for monitor in monitors:
#        old_uid = str(monitor.uid.encode("utf-8"))
#        if "refer" in old_uid:
#            new_uid = old_uid.split("&")[0].split("=")[1]
#            monitor.update(monitor.uid:new_uid)
#            session.commit()
#            session.flush()
#            print "update one"
#    print "finished"
    
    
if __name__ == "__main__":
   
    #refine_uid()
    #crawl_monitor_uidAndName_Parallel()
    #crawl_monitor_uidAndName_single_ForParallel()
    #refine_uid()
    #crawl_expert_recent10days_parallel()
    #crawl_expert_recent10days_single_forParallel_improved(["XXXXXX","XXXXXX"])
    