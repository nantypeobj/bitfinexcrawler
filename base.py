# -*- coding: utf-8 -*-
"""
Created on Sat Sep 17 11:42:54 2016

@author: Administrator
"""
import numpy as np
import pandas as pd
import random
import re
import math
import datetime
import string
import time
import pytz
from tzlocal import get_localzone
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

def nounce():
    return time.time()*1000

#一个变量是否为int
#空的默认定义为：int,np.int16,np.int32,np.int64,np.int8
#var--任意变量
def isInt(var,rule=[int,np.int16,np.int32,np.int64,np.int8]):
    return type(var) in rule


#一个变量是否为int
#空的默认定义为：int,np.int16,np.int32,np.int64,np.int8
#var--任意变量
def isDf(var,rule=[pd.core.frame.DataFrame,pd.core.series.Series]):
    for rl in rule:
        if isinstance(var,rl):
            return True
    return False
    
    #一个pd.df是否为空
#空的定义为：isEmptyvar 或 一个维度为0
#df---pandas df
#返回bool
def isEmptydf(df):
    return  (df is None) or min(df.shape)==0 

def isNull(var):
    return  (var is None) or (math.isnan(var))
    
def isEmptyvar(var,rule=[[],{},'',()]):
    try:
        return isEmptydf(var) if isDf(var) else ((var in rule) or (isNull(var)))
    except TypeError:
        return False

#判断输入变量是否为iterable
#exclude_type:hasattr(var, '__iter__')为true的情况下需要返回False的特殊type
def isIter(var,exclude_type=[str,dict,pd.DataFrame]):
    if hasattr(var, '__iter__'):
        return (isEmptyvar(exclude_type) or (not type(var) in exclude_type))
    else:
        return False

#任意变量转为list
def toList(var):
    if type(var)==pd.Series:
        return var.tolist()
    else:
        return var if isIter(var) else [var] 
    

def datetime_toStr(var,dateformat="%Y%m%d"):
    try:
       return var.strftime(dateformat)   
    except:
        return var

def timestamp_toStr(var,dateformat="%Y%m%d %H:%M:%S",timezone='Asia/Shanghai'):
    try:
       return datetime_toStr(timestamp_toDatetime(var,timezone=timezone),dateformat=dateformat)
    except:
        return var

def timestamp_toDatetime(var,timezone='Asia/Shanghai'):
    check_empty(var)
    if len(str(int(float(var))))>10:
        var=var*1.0/(10**(len(str(int(float(var))))-10))
    
    if timezone:
        localtz = pytz.timezone(timezone)
        f=lambda v:datetime.datetime.fromtimestamp(v,localtz)
    else:
        f=lambda v:datetime.datetime.fromtimestamp(v)
        
    if isIter(var):
        return [f(v) for v in var]
    else:
        return f(var)

def datetime_toTimestamp(t,timezone='Asia/Shanghai',timelen=13):
    if not timezone:
        tz=get_localzone()
    else:
        tz=pytz.timezone(timezone)
    t=(t.replace(tzinfo=tz).timestamp()+6*60)
    if len(str(int(float(t))))<timelen:
        t=t*(10**(timelen-len(str(int(float(t))))))
    return t

#将str转为datetime
#var--str变量   
#dateformat---转换格式
def str_toDatetime(var,dateformat="%Y%m%d"):
    try:
        return parse(var)
    except:
        return var


def toDatetime(var,dateformat="%Y%m%d"):
    try:
        return parse(var)
    except:
        return var

def str_toTimestamp(var,dateformat="%Y%m%d %H:%M:%S"):
    if type(var)!=str:
        return var
    return datetime_toTimestamp(str_toDatetime(var,dateformat=dateformat))



def list_rmvbyval(l,val):
    vallist=toList(val)
    return [v for v in l if not v in vallist]
    
def list_isEqual(l1,l2):
    l1,l2=toList(l1),toList(l2)
    
    if len(l1)!=len(l2):
        return False
    
    for i,j in zip(l1,l2):
        if i!=j:
            return False
    
    return True
        
    
#将int转为str
#1.直接将int转为str
#2.规定转换后str的长度，int不足的部分手工填补
#var---int变量，包括int、np.int
#str_len--int,自定义转换后str的长度
#str_pad_direction--int(0,1)手工填充str位数的方向，0左1右
#str_pad---str,填充内容
def int_toStr(var,str_len=None,str_pad_direction=0,str_pad='0'):
    if not isInt(var):
        return var
    
    str_var=str(var)
    
    if str_len is None:
        return str_var
    
    if str_pad_direction==0:
        return str_pad*(str_len-len(str_var))+str_var
    else:
        return str_var+str_pad*(str_len-len(str_var))

def write_txt(content,filename):
    with open(filename, "a") as text_file:
        text_file.write("\n")
        text_file.write(str(content))

def write_csv(df,filenam,filetype):
    if filetype=='csv':
        df.to_csv(filenam)
    elif filetype=='txt':
        df.to_csv(filenam,sep=' ', mode='a')
    else:
        pass
    
def get_currenttime_asstr(dateformat="%Y%m%d %H:%M:%S"):
    now=int(time.time())
    return timestamp_toStr(now,dateformat=dateformat)

def get_today_asstr():
    return get_currenttime_asstr(dateformat="%Y%m%d")

def calc_daysbtwdates(date1,date2,dateformat="%Y%m%d",daytype=0):
    if daytype==0:
        return (str_toDatetime(date1)-str_toDatetime(date2)).days
    elif daytype==1:
        return (str_toDatetime(date1).month-str_toDatetime(date2).month)
    else:
        return (str_toDatetime(date1).year-str_toDatetime(date2).year)

#daytype=1:如果间隔不为整年、整月，则按照天数计算 daytype=0 直接减
def calc_datesbtwdates(date1,date2,daytype=1):
    d1,d2=str_toDatetime(date1),str_toDatetime(date2)
    if daytype==1 and d1.day-d2.day!=0:
        return 0,0,(d1-d2).days
    else:
        return d1.year-d2.year,d1.month-d2.month,d1.day-d2.day


#输入检查,不满足需求则raise exception
#不满足需求checkfunc返回True
#var是一个单独的变量
def check_input(checkfunc,errorcontent,inputs):
    inputs=toList(inputs)
    for var in inputs:
        if checkfunc(var):
            print (var)
            raise(ValueError(errorcontent))
    return True
    
def check_empty(var,errorcontent='ERROR:返回值为空'):
    return check_input(isEmptyvar,errorcontent,[var])

def check_include(var,invar,errorcontent='ERROR:输入值不在规定范围内'):
    return check_input(lambda v:not (v in invar),errorcontent,var)
    
def isLen(var,l=1):
    if not isIter(var):
        return True if l==1 else False
    else:
        return len(var)==l

#检查变量的长度是否满足需求
#var--一个变量
def check_len(var,l=1):
    return check_input(lambda x : not isLen(x,l),'length doesnt match',[var])
#检查变量的值是否等于指定值
def check_equal(var,val,errorcontent='value doesnt equal'):
    return check_input(lambda x : var!=val,errorcontent,[var])

#将一个变量的长度拓展为指定长度
#如果变量长度=1，拓展为指定长度的list；如果变量长度》1，和指定长度相等就返回，不相等就异常
def var_extendlen(var,l=1):
    if isLen(var):
        var=toList(var) 
        return var*l     
    else:
        if check_len(var,l):
            return var

def list_getexclision(l1,l2):
    return list(set(l1) - set(l2))

def dict_mulkeys(dic,keys):
    return [dic[k] for k in keys]

def dict_partbykey(dic,keys):
    return dict([(k,dic[k]) for k in keys])
    
def random_select(l):
    return random.choice(l)

#旧版日期计算
def date_togap(date,dateformat="%Y%m%d",gap_type=0,gap_val=0):
    if date is None:
        return date
    gap_types=['days','months','years','minutes','seconds']
    #转化为string格式
    tar_date=str_toDatetime(date,dateformat) + relativedelta(**{gap_types[gap_type]:gap_val})
    return datetime_toStr(tar_date,dateformat)

#新版日期计算
def date_togapn(date,dateformat="%Y%m%d",years=0,months=0,days=0,minutes=0,seconds=0):
    
    if date is None:
        return date
        
    if minutes!=0 or seconds!=0:
        dateformat=dateformat+' %H:%M:%S'
        
    gap_types={'days':days,'months':months,'years':years,'minutes':minutes,'seconds':seconds}
    #转化为string格式
    tar_date=str_toDatetime(date,dateformat) + relativedelta(**gap_types)
    return datetime_toStr(tar_date,dateformat)
    
def time_togapn(tar_time,years=0,months=0,days=0,minutes=0,seconds=0,microseconds=0):
    
    if tar_time is None:
        return tar_time
    if type(tar_time)==str:
        tar_time=str_toDatetime(tar_time,dateformat="%Y%m%d %H:%M:%S") 
        
    gap_types={'days':days,'months':months,'years':years,'minutes':minutes,
    'seconds':seconds,'microseconds':microseconds}

    return datetime_toStr(tar_time + relativedelta(**gap_types),dateformat="%Y%m%d %H:%M:%S")

#0~9依然是0~9，大于9的从A开始记作10
def str_toASCIIint(s):
    try:
        return int(s)
    except ValueError:
        return ord(s)-ord('A')+10

#0~9依然是0~9，大于9的从A开始记作10
def asciiint_toStr(i):
    if i<10:
        return str(i)
    else:
        return chr(i+ord('A')-10)
        

def time_togap(time=None,dateformat="%Y%m%d %H:%M:%S",gap_type=4,gap_val=0):
    time=get_currenttime_asstr(dateformat) if time is None else time
    return date_togap(time,dateformat=dateformat,gap_type=gap_type,gap_val=gap_val)

def date_isbigger(date1,date2,dateformat="%Y%m%d"):
    return str_toDatetime(date1)>str_toDatetime(date2) 

def to_sametype(l1,l2):
    l1,l2=toList(l1),toList(l2)
    return map(lambda x,y:type(x)(y),l1,l2)
    

#对于df的每一个元素实行函数appfunc
def df_applytoeach(df,appfunc,*args,**kwards):
    if not isDf(df):
        raise(ValueError('输入变量不是pd.df格式'))
    if isEmptyvar(df):
        return df
    else:
        return df.apply(lambda col:[appfunc(x,*args,**kwards) for x in col])

def set_defaultinput(inputs,defaultvalus):
    return [y if (x is None) else x for x,y in zip(inputs,defaultvalus)]

#连续的相同长度的子列表为一段切割
def lists_groupbylen(lists):
    lenlist=pd.Series([len(x) for x in lists])

    group_staindex=pd.Series(lenlist.index[lenlist!=lenlist.shift()]).astype(int)
    
    group_endindex=group_staindex.shift(-1).fillna(len(lists)).astype(int)

    
    return [lists[i:j]for i,j in zip(group_staindex,group_endindex)]

def calc_str(s1,s2):
    if (not re.search('\.',s1)) and (not re.search('\.',s2)):
        strlen=len(s1)
        res=int(s1)+int(s2)
        return int_toStr(res,str_len=strlen)
    else:
        return str(float(s1)+float(s2))

#生成一段随机字符
def random_str(length,hasletter=True):
    chars=string.ascii_uppercase + string.digits +string.ascii_lowercase if hasletter \
        else string.digits
    return ''.join(random.choice(chars) for _ in range(length))

#核对dataframe的格式
def df_settype(df,typedict):
    
    funcmap={'numeric':pd.to_numeric,'datetime':pd.to_datetime,
    'string':lambda col: [str(x) for x in col]}
    
    for k,v in typedict.items():
        df[k]=funcmap[v](df[k])
    
    return df
    
    


    #抽取df中的行返回
    #若所有的行都不存在，返回None
    #1.按照index值抽取行，select_rows为index中的值；
    #2.按照row的值抽取行，select_rows为行的值；
def df_extractrows(df,select_rows,select_rows_operaters=0):
    df=pd.DataFrame(df)
    if isEmptydf(df) or isEmptyvar(select_rows):
        return df
        
    select_rows_operaters=var_extendlen(select_rows_operaters,len(select_rows))
        
    ops=[lambda sel,v:v.isin(sel),
        lambda sel,v:(v>= sel[0]) & (v<=sel[1])]
        
    indexnam=df.index.name

    for (colnam,rowval),opi in zip(select_rows.items(),select_rows_operaters):
        if isEmptyvar(rowval):
            continue
        row_vals=toList(rowval)
        try:
            df=df.loc[ops[opi](row_vals,df.index)] if colnam==indexnam \
                else df.loc[ops[opi](row_vals,pd.Series(df[colnam]))]
        except (KeyError,IndexError):
            continue
    return df

def syb_formatchn(s,decode='GB2312',encode='utf-8'):
    try:
        return unicode(s,encoding=decode).encode(encode)
    except (TypeError,UnicodeDecodeError):
        return s
    
def syb_formatchn_df(df,colnams,decode='GB2312',encode='utf-8'):
    colnams=toList(colnams)
    f=lambda it : [syb_formatchn(s) for s in it]
    df[colnams]=df[colnams].apply(f)
    return df

def syb_formatexception(msg):
    raise Exception(syb_formatchn(msg))
    
def get_nearvalue(val,unit=-1):
    val=float(val)
    digitlen_behind=len(re.findall('\.([0-9]*)',str(val))[0])
    return val+unit*(10**(-digitlen_behind))
    
           #input--['k1','k2'],[1,2]
        #output--[{'k1': 1}, {'k2': 2}]
def lists_2dictlists(key_list,val_list):
    f=lambda k,v:lists_2dict(k,v)
    return map(f,key_list,val_list)
        
        #input--['k1','k2'],[1,2]
        #output--{'k1': 1, 'k2': 2}
def lists_2dict(key_list,val_list):
    if not isIter(key_list):
        return {key_list:val_list}
    val_list=any_2list(val_list)
    return dict(zip(key_list,val_list))

def any_2list(obj):
    return obj if isIter(obj) else [obj] 
