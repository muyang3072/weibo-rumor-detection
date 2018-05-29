# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 09:26:47 2018

@author: imuyang
"""
import time
import random
from utils import *
import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool
from itertools import izip_longest

from model import Expert_Post,Monitor_Post,engine
from selenium.common.exceptions import NoSuchElementException

from sqlalchemy import *
from sqlalchemy.orm import *

DBSession = sessionmaker(bind=engine)
session = DBSession()



def crawl_experts_name(b):
    month_bang_xpath = "//*[@id='Pl_Wemedia_H5_Rank']/article/section[1]/div/div/div[3]/a"
    domain_list = [
            "hulianwang",
            "kexue",
            "lishi",
            "junshi",
            "renwen",
            "gaoxiao",                            
            "yangsheng",
            "jianshen",
            "tiyu",
            "meishi",
            "lvyou",
            "caijing",
            "yiliao",
            "dushu",
            "fangdichan",
            "qiche",
            "jiaoyu",
            "falv",
            "muying",
            "kejiguancha",
            ]
    url_list = ["http://v6.bang.weibo.com/czv/"+domain+"?period=month" for domain in domain_list]
    for url in url_list:
        b.get(url)
        time.sleep(3)
        for i in range(4):
            js = "window.scrollTo(0,document.body.scrollHeight);"
            b.execute_script(js)
            time.sleep(3)
            
        #user_list_xpath = "//section[@node-type='listUserCzv']/div[1]//div"
        user_name_xpath = ".//section[@node-type='listUserCzv']/div[1]//div//h3"
        user_list = b.find_elements_by_xpath(user_name_xpath)
        for user in user_list:
            print user
            user_name = user.text
            print "用户名: ",user_name
            try:
                write_expert_name().send(user_name)
            except StopIteration:
                continue
            

def crawl_experts_uid(b):
    

    name_list = session.query(Expert_Post.name).filter(Expert_Post.uid == None).all()
    name_list = [str(res[0].encode("utf-8")) for res in name_list]
    session.commit()

    for name in name_list:
        sleep_time = random.randint(10,40)
        time.sleep(sleep_time)
        url = "http://s.weibo.com/user/"+name
        b.get(url)
        time.sleep(1.5)
        try:
            uid = b.find_element_by_xpath("//*[@id='pl_user_feedList']/div[1]/div/div/div/div[1]/div[3]/p[1]/a[1]").get_attribute("usercard").split("&")[0].split("=")[1]
            print "usercard: ",uid
            write_expert_uid().send((name,uid))
        except NoSuchElementException:
            print "nosuchelementexception"
            expert = session.query(Expert_Post).filter_by(name = name).scalar()
            print "before delete"
            session.delete(expert)#因为这块儿的混账逻辑，导致本来数据库应该有1800个用户，后来只剩下560个了
            print "deleted!"
            session.commit()
            continue
        except StopIteration:
            print "stopiteration"
            continue

def crawl_experts_uid_single_forParallel(task_list):
    username,password = task_list[1]
    name_list = task_list[0]
    b = LoginWeibo(username,password)
    for name in name_list:
        sleep_time = random.randint(5,10)
        time.sleep(sleep_time)
        url = "http://s.weibo.com/user/"+name
        b.get(url)
        time.sleep(1.5)
        try:
            uid = b.find_element_by_xpath("//*[@id='pl_user_feedList']/div[1]/div/div/div/div[1]/div[3]/p[1]/a[1]").get_attribute("usercard").split("&")[0].split("=")[1]
            print "usercard: ",uid
            write_expert_uid().send((name,uid))
        except NoSuchElementException:
            #print "nosuchelementexception"
            expert = session.query(Expert_Post).filter_by(name = name).scalar()
            #print "before delete"
            session.delete(expert)
            #print "deleted!"
            session.commit()
            continue
        except StopIteration:
            #print "stopiteration"
            continue
    
    
def crawl_experts_uid_parallel():
    pool = ThreadPool(2)
    name_list = session.query(Expert_Post.name).filter(Expert_Post.uid == None).all()
    name_list = [str(res[0].encode("utf-8")) for res in name_list]
    session.commit()
    l = len(name_list)
    cut_list = [name_list[:l/2],name_list[l/2:]]
    account_list = (["XXXXXX","XXXXXX"],["XXXXXX","XXXXXX"])
    task_list = [[cut_list[0],account_list[0]],[cut_list[1],account_list[1]]]
    pool.map(crawl_experts_uid_single_forParallel,task_list)
        

def crawl_expert_recent10days_single_forParallel(task_list):
    
    uid_list = task_list[0]
    print "get task numbers: ",len(uid_list)
    username,password = task_list[1]
    b = LoginWeibo(username,password)
    start_month = "3"
    start_day = "17"
    end_month = "3"
    end_day = "27"#这里的时间区间，根据自己需要指定即可
    empty_xpath = "//div[@class='WB_empty']"
    content = ""
    exit_flag = False
    for uid in uid_list:
        try:
            url = "https://weibo.com/"+str(uid)#+"?refer_flag=1087030101_417"
            print url
            sleep_time = random.randint(60,120)
            time.sleep(sleep_time)
            b.get(url)
            print u"准备访问用户主页......",uid
            time.sleep(10)
            try:
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
                    write_expert_content().send((uid,content))
                except StopIteration:
                    continue
            else:
                sleep_time = random.randint(120,360)
                time.sleep(sleep_time)
        except:
            try:
                if content != "":
                    write_expert_content().send((uid,content))
                else:
                    continue
            except StopIteration:
                continue
    b.quit()


def crawl_expert_recent10days_parallel():
    uid_list = session.query(Expert_Post.uid).filter(Expert_Post.content == None).all()
    uid_list = [res[0] for res in uid_list]
    session.commit()
    chunk_list = lambda a_list, n: izip_longest(*[iter(a_list)]*n)
    cut_list = list(chunk_list(uid_list, 100))
    account_list = [["XXXXXX","XXXXXX"],["XXXXXX","XXXXXX"]] * 500
    task_list = [[cut_list[i],account_list[i]] for i in range(len(cut_list))]
    pool = ThreadPool(2)
    pool.map(crawl_expert_recent10days_single_forParallel,task_list)
    
    
def crawl_monitor_uidAndName_single_ForParallel(task_list):
    username,password = task_list[1]
    domain_list  = task_list[0]
    b = LoginWeibo(username,password)
    b.get(domain_list)
    domain_more_xpath = "//a[@class='item_link S_txt1' and @node-type='fold_btn']"
    elems_xpath = "//li[@class='follow_item S_line2']"
    for domain in domain_list:
        b.find_element_by_xpath(domain).click()
        elem_list = b.find_elements_by_xpath(elems_xpath)
        for elem in elem_list:
            uid = elem.get_attribute("usercard")
            name = elem.get_attribute("title")
            try:
                write_monitor_uidAndName().send((uid,name))
            except StopIteration:
                continue
   
    
def crawl_monitor_uidAndName_Parallel():
    name_list = session.query(Monitor_Post.name).all()
    name_list = [res[0] for res in uid_list]
    session.commit()
    
    cut_list = []
    account_list = (["XXXXXX","XXXXXX"],["XXXXXX","XXXXXX"])
    task_list = [[cut_list[0],account_list[0]],[cut_list[1],account_list[1]]]
    pool = ThreadPool(2)
    pool.map(crawl_monitor_uidAndName_single_ForParallel,task_list)


def crawl_user_lastWindow(b,uid):
    pass


def crawl_post_info(b,mid):
    pass

if __name__ == "__main__":
    #读取配置文件(一种读取配置文件而非硬编码的实现，但代码中没有使用)
    # cf = ConfigParser.ConfigParser()
    # cf.read("crawler.conf")
    # username1 = cf.get("crawler","username1")
    # password1 = cf.get("crawler","password1")
    # find_people_url = cf.get("crawler","find_people_url")
    # find_expert_url = cf.get("crawler","find_expert_url")
    # find_topic_url = cf.get("crawler","find_topic_url")
    #登录
    #b = LoginWeibo(username1,password1)
    #爬取专家信息
    #crawl_experts_name(b)
    #crawl_experts_uid(b)
    #crawl_experts_uid_parallel()
    #crawl_expert_recent10days_parallel()
    #crawl_expert_recent10days_single()
