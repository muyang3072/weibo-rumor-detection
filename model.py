# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 13:50:56 2018

@author: imuyang
"""

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base


#创建对象的基类
Base = declarative_base()

#定义微博post对象：
class Post(Base):
    #表的名字
    __tablename__ = "Post"
    
    #表的结构
    
    mid = Column(String(50),primary_key=True)
    uid = Column(String(50))
    content = Column(String(1000))
    
    forward_number = Column(String(50))
    like_number = Column(String(50))
    comment_number = Column(String(50))
    time_window = Column(String(50))
    
    
class Train_Window(Base):
    __tablename__ = "Train_Window"
    time_window = Column(String(50),primary_key=True)
    feature_list = Column(Text(100000))
    mass_list = Column(Text(100000))
    distance_list = Column(Text(100000))
    velocity_list = Column(Text(100000))
    accelerate_list = Column(Text(100000))
    momentum_list = Column(Text(100000))
    
    
    
class Test_Window(Base):
    __tablename__ = "Test_Window"
    time_window = Column(String(50),primary_key=True)
    feature_list = Column(Text(100000))
    mass_list = Column(Text(100000))
    distance_list = Column(Text(100000))
    velocity_list = Column(Text(100000))
    accelerate_list = Column(Text(100000))
    momentum_list = Column(Text(100000))
    burst_flag = Column(Text(100000))
    
    
class Expert_Post(Base):
    __tablename__ = "Expert_post"
    uid = Column(String(50),unique = True)
    name = Column(String(50),primary_key=True,unique = True)
    content = Column(String(100000))
    
    
class Monitor_Post(Base):
    __tablename__ = "Monitor_Post"
    uid = Column(String(50),unique = True)
    name = Column(String(50),primary_key=True,unique = True)
    content = Column(Text(1000000))
    follower_number = Column(String(20))
    followee_number = Column(String(20))
    post_number = Column(String(20))
    is_verified = Column(String(10))
    
    
class Monitor_Post_1(Base):
    __tablename__ = "Monitor_Post_1"
    uid = Column(String(50),unique = True)
    name = Column(String(50),primary_key=True,unique = True)
    content = Column(Text(100000))
    follower_number = Column(String(20))
    followee_number = Column(String(20))
    post_number = Column(String(20))
    is_verified = Column(String(10))
    
    
class Monitor_Post_2(Base):
    __tablename__ = "Monitor_Post_2"
    uid = Column(String(50),unique = True)
    name = Column(String(50),primary_key=True,unique = True)
    content = Column(Text(100000))
    follower_number = Column(String(20))
    followee_number = Column(String(20))
    post_number = Column(String(20))
    is_verified = Column(String(10))

  
class Monitor_Post_3(Base):
    __tablename__ = "Monitor_Post_3"
    uid = Column(String(50),unique = True)
    name = Column(String(50),primary_key=True,unique = True)
    content = Column(Text(100000))
    follower_number = Column(String(20))
    followee_number = Column(String(20))
    post_number = Column(String(20))
    is_verified = Column(String(10))



class Topic(Base):
    __tablename__ = "Topic"
    
    time_window = Column(String(50),primary_key = True)
    topics = Column(String(10000))
    
engine = create_engine("mysql+pymysql://root:admin@127.0.0.1:3306/paper_lab?charset=utf8mb4")#如果向自定义数据库名称，替换“paper_lab”即可。对密码"admin"也是同理
Base.metadata.create_all(engine)
