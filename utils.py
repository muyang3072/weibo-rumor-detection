# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 13:26:01 2018

@author: imuyang
"""
import time
from functools import wraps
import codecs

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchElementException

from model import Expert_Post,Monitor_Post,Post,Topic,engine
from sqlalchemy import *
from sqlalchemy.orm import *



def LoginWeibo(username = "XXXXXX", password = "XXXXXX"):
    try:
        print u'准备登陆login.sina.com.cn网站...'
        browser = webdriver.Chrome()
        browser.maximize_window()
        browser.get("http://login.sina.com.cn/")
        elem_user = browser.find_element_by_name("username")
        elem_user.send_keys(username) #用户名
        elem_pwd = browser.find_element_by_name("password")
        elem_pwd.send_keys(password)  #密码
        elem_pwd.send_keys(Keys.RETURN)
        time.sleep(30)
        print browser.current_url
        print u'登陆成功...'
        return browser
    except Exception,e:      
        print "Error: ",e
    finally:    
        print u'End LoginWeibo!\n\n'
        
        
def has_element_byXpath(driver,xpath):
    flag = False
    try:
        driver.find_element_by_xpath(xpath)
        flag = True
    except NoSuchElementException:
        #print "找不到元素"
        flag = False
    return flag
        
        
def coroutine(func):
    @wraps(func)
    def primer(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return primer


@coroutine
def write_expert_name():
    user_name = yield
    print "write_expert_name received: "+user_name
    new_expert = Expert_Post(name = user_name)
    session.merge(new_expert)
    session.commit()
    session.flush()
    f = codecs.open("expert_name.txt","a","utf-8")
    f.write(user_name+"\n")
    f.close()
    
@coroutine
def write_expert_uid():
    name,uid = yield
    print "write_expert_uid received: "+name,uid
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    expert = session.query(Expert_Post).filter_by(name= name).scalar()
    expert.uid = uid
    session.merge(expert)
    session.commit()
    session.flush()
    f = codecs.open("expert_uid.txt","a","utf-8")
    f.write(uid+"\n")
    f.close()
    
    
@coroutine
def write_expert_content():
    uid,content = yield
    print "write_expert_content received: \n"+uid+"\n"+content
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    expert = session.query(Expert_Post).filter_by(uid= uid).scalar()
    content = content.encode("utf-8")
    print content
    print type(content)
    expert.content = content
    session.commit()
    session.flush()

    
@coroutine
def write_monitor_uidAndName():
    uid,name = yield
    print "write_expert_content received: \n"+uid+"\n"+name
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    new_monitor = Monitor_Post(uid = uid,name = name)
    session.merge(new_monitor)
    session.commit()
    session.flush()
    f = codecs.open("monitor_uidAndName.txt","a","utf-8")
    f.write(uid+"\n"+name+"\n")
    f.close()
    
    
@coroutine
def write_monitor_repi():
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    uid,follower_num,followee_num,post_num,is_verified = yield
    print "write_expert_content received: \n"+uid+"\n"+follower_num+"\n"+followee_num+"\n"+post_num+ "\n" + is_verified
    mp = session.query(Monitor_Post).filter_by(uid = uid).scalar()
    mp.follower_number = follower_num
    mp.followee_number = followee_num
    mp.post_number = post_num
    mp.is_verified = is_verified
    session.merge(mp)
    session.commit()
    session.commit()

    
    
@coroutine
def write_monitor_content():
    uid,content = yield
    print "write_monitor_content received: \n"+uid+"\n"+content
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    monitor = session.query(Monitor_Post).filter_by(uid= uid).scalar()
    cur_content = str(content.encode("utf-8"))
    if monitor.content == None:
        monitor.content = cur_content
    else:
        old_content = str(monitor.content.encode("utf-8"))
        new_content = old_content + "\n" + cur_content
        monitor.content = new_content
    session.commit()
    session.flush()
    

@coroutine
def write_post_perWindow():
    mid,uid,forward_num,like_num,comment_num,time_window,content = yield
    print "write_post_perWindow received: \n"+mid+"\n"+uid+"\n"+forward_num+"\n"+like_num+"\n"+comment_num+"\n"+time_window+"\n"+content
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    content = content.encode("utf-8")
    new_post = Post(mid = mid,uid = uid,forward_number = forward_num,like_number = like_num,comment_number = comment_num,time_window = time_window,content = content)
    session.merge(new_post)
    session.commit()
    session.flush()
    
    
@coroutine
def write_topic_perWindow():
    time_window,topics = yield
    print "write_topic_perWindow received: \n" + time_window + "\n" + topics
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    topics = Topic(time_window = time_window,topics = topics)
    session.merge(topics)
    session.commit()
    session.flush()
    
    
