
from DatabaseInterface import DatabaseInterface
import base
import time

def getdata(pair,bolllen):
    db=DatabaseInterface('data_bitmex')
    data=db.db_find([],'KLINE5m',filter_dic={'pair':pair},
                     sort=[('time', -1)],limit=bolllen)
    data.index=[base.timestamp_toDatetime(float(t)) for t in data['time']]
    data=data.sort_values('time')
    return data


def updatesignal(pair,timeframe,usedb,exchangename=None):
    db=DatabaseInterface(usedb)
#    pair,timeframe=format_input(pair,timeframe,exchangename)
    db.db_updateone({'timeframe':timeframe,'pair':pair},{'sig':'1'},'DATAUPDATESIG',upserts=True)

def format_input(pair,timeframe,exchangename=None):
    if exchangename=='bitmex' and pair=='XBTUSD':
        pair='BTCUSD'
    else:
        pair=pair.replace('_','').upper()
    tf1=re.findall(r'\d+', timeframe)[0]
    tf2=timeframe[timeframe.index(tf1[-1])+1]
    return pair,tf1+tf2



time.sleep(15)
pair='BTCUSD'
bolllen=6
timeframe='30m'
usedb='data_bitmex'
data=getdata(pair,bolllen)
db=DatabaseInterface(usedb)

if data.index[-1].minute not in [55,25]:
    print(data)
    assert False,str(data.index[-1].minute)    
else:
    insertdata={'close':float(data['close'].iloc[-1]),'time':float(data['time'].iloc[0]),'open':float(data['open'].iloc[0]),'high':float(data['high'].max()),'low':float(data['low'].min()),'volume':float(data['volume'].sum()),'pair':pair}
    lastdata=db.db_find([],'KLINE30m',filter_dic={'pair':pair},
                     sort=[('time', -1)],limit=1).iloc[0,:]
    insertdata['pctchange']=(data['close'].iloc[-1]-lastdata['close'])*100.0/lastdata['close']
    print('bitmex 30m data updated')
    print(insertdata)
    db.db_insertone(insertdata,'KLINE30m')
    updatesignal(pair,timeframe,usedb) 

