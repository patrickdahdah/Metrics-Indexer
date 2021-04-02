from lib.configs import settings, balances_table_name, economics_table_name, balances_metrics_table_name, transaction_metrics_table_name, dfAddresses, addrList
from database import sql
import time
import requests
import calendar
import datetime 
import pandas as pd

URL=settings["baseUrl"]
priceUrl = settings["priceUrl"]
DATE_PAST_PRICES = pd.to_datetime("2019-09-18", format = "%Y-%m-%d")
CSVPRICE=pd.read_csv('data/price.csv') 
CSVPRICE["timestamp"]=pd.to_datetime(CSVPRICE['timestamp'], format = "%m/%d/%Y")

def fetchGet(url: str):
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise requests.exceptions.RequestException('The request generates an unexpected response  \
            Status code= %d' %(response.status_code) )
    return response

def fetchPost(url: str, body: dict): #function never used
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        raise Exception('The request generates an unexpected response\n \
            Status code= %d' %(response.status_code) )
    return response

def getBalance(address: str): 
    response= fetchGet("{url}/account/{addr}".format(url=URL, addr=address))
    info=response.json()
    return info["balance"]/1000000 #returns in algos not microalgos

def getStatus(address: str):
    response= fetchGet("{url}/account/{addr}".format(url=URL, addr=address))
    info=response.json()
    return info["status"]

def getPrice(timestamp): #timestamp in YEAR-MM-DD in pandas datatime
    timestampYMD = timestampToStringYMD(timestamp)
    if pd.to_datetime(timestampYMD) < DATE_PAST_PRICES:
        timestampYMD = pd.to_datetime(timestampYMD)
        return CSVPRICE.loc[CSVPRICE['timestamp'] == timestampYMD, 'price'].item()
    else:
        responsePrice = fetchGet("{url}/algo-usd/history?since={since}&until={until}".format(url=priceUrl,since=timestamp-43200,until=timestamp))
        jsonPrice=responsePrice.json()
        pricesData = jsonPrice["history"]
        if pricesData:
            return pricesData[-1]["close"]
        else:
            raise requests.exceptions.RequestException('The request has no data  \
            Status code= %d' %(responsePrice.status_code) ) 

def getPriceRows(timestamp): #timestamp in YEAR-MM-DD in pandas datatime

    responsePrice = fetchGet("{url}/algo-usd/history?since={since}&until={until}".format(url=priceUrl,since=timestamp,until=timestamp+86399))
    jsonPrice=responsePrice.json()
    pricesData = jsonPrice["history"]

    return pricesData


def timestampToString(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t))

def timestampToStringYMD(t): #same as above but in Y m d format
    return time.strftime("%Y-%m-%d", time.gmtime(t))

def stringToTimestamp(t):
    return int(calendar.timegm(time.strptime(str(t), "%Y-%m-%d %H:%M:%S")))

def stringYMDToTimestamp(t):
    return int(calendar.timegm(time.strptime(str(t), "%Y-%m-%d")))

def dateformatcsv(t): #when importing csv, tha date format changes so this functions corrects it
    return datetime.datetime.strptime(t, "%m/%d/%Y").strftime("%Y-%m-%d")

def checkForAddresses(transactionsDataFrame): #checks for new addresses that most be excluded from the calculations for indicators tables
    for i in transactionsDataFrame.index: 
        if (transactionsDataFrame['from'][i] in addrList) and (pd.isnull(transactionsDataFrame['close'][i]) is not True) and  (transactionsDataFrame['close'][i] not in addrList): #check if a new address must be added by checking the close to
            addrList.append(transactionsDataFrame['close'][i])

def checkForBigTransactions(transactionsDataFrame): 
    a = []
    for i in transactionsDataFrame.index: 
        if (transactionsDataFrame['from'][i] in addrList) and (transactionsDataFrame['amount'][i] > 15000000) and  (transactionsDataFrame['to'][i] not in addrList):
            inf = {transactionsDataFrame["txid"][i] : transactionsDataFrame['to'][i]}
            a.append(inf)
    return a

def AddCSVinfo(AddrDF): #fills the Address.csv columns status|balance|Aindex|txid  
    global dfAddresses
    originalList = dfAddresses['Address'].tolist()
    listWithMore = AddrDF["addr"].tolist()

    for i in addrList: 
        # print(i)
        if i in listWithMore:
            if i in originalList: #normal address
                dfAddresses.loc[dfAddresses['Address'] == i, 'balance'] = AddrDF.loc[AddrDF['addr'] == i, 'balance'].tolist()[0]
                dfAddresses.loc[dfAddresses['Address'] == i, 'Aindex'] = AddrDF.loc[AddrDF['addr'] == i, 'Aindex'].tolist()[0]
                dfAddresses.loc[dfAddresses['Address'] == i, 'txid'] = AddrDF.loc[AddrDF['addr'] == i, 'txid'].tolist()[0]
                dfAddresses.loc[dfAddresses['Address'] == i, 'status'] = getStatus(i)
            else: #address founded by checkForAddresses() function
                # print(i)
                row = {'Address': i, 
                        'Owner': "tracked", 
                        'status': getStatus(i),
                        'balance': AddrDF.loc[AddrDF['addr'] == i, 'balance'].tolist()[0],
                        'Aindex' : AddrDF.loc[AddrDF['addr'] == i, 'Aindex'].tolist()[0],
                        'txid' : AddrDF.loc[AddrDF['addr'] == i, 'txid'].tolist()[0]
                        }
                dfAddresses = dfAddresses.append(row, ignore_index = True)

            
        else: #genesis/block 0   no transactions made 
            dfAddresses.loc[dfAddresses['Address'] == i, 'balance'] = getBalance(i)
            dfAddresses.loc[dfAddresses['Address'] == i, 'Aindex'] = 0
            dfAddresses.loc[dfAddresses['Address'] == i, 'status'] = getStatus(i)
    return dfAddresses

def transactionsToAdrrs(transactionsDF): #transactions to unique addresses list with their corresponding last balance and Aindex
    fromTransactions= transactionsDF[['index',  'timestamp', 'txid', 'from', 'from_balance', 'from_index' ]].rename(columns = {'from' : 'address', 'from_balance' : 'balance','from_index':'address_index'})#split the `from` addrs columns

    toTransactions=transactionsDF[['index',  'timestamp', 'txid', 'to', 'to_balance', 'to_index' ]].rename(columns = {'to' : 'address', 'to_balance' : 'balance','to_index':'address_index'})#split the `to` addrs columns

    closeTransactions=transactionsDF[['index',  'timestamp', 'txid', 'close', 'close_balance', 'close_index' ]].rename(columns = {'close' : 'address', 'close_balance' : 'balance','close_index':'address_index'})#split the `close` addrs columns
    closeTransactions.dropna(inplace = True)
    closeTransactions['address_index'] = closeTransactions['address_index'].astype(int)

    addrDF=fromTransactions.append([toTransactions,closeTransactions])

    addrDF.sort_values("index", inplace = True) #sort transaction index by ASC
    addrDF.drop_duplicates(subset="address" , inplace = True, keep="last") #keep last txinfo (last balance info)

    return addrDF

def addRealizedCapColumn(addrsDF, cs): #merger cs table to add price column and calculate realized price column
    addrsDF=pd.merge(addrsDF,cs, on="timestamp")
    addrsDF['realizedCapAddr']= addrsDF["balance"]*addrsDF["price"]
    addrsDF.set_index("timestamp", inplace=True)
    addrsDF.sort_index(inplace=True)

    return addrsDF

def addRealizedCapColumn2(addrsDF, timestamp): #get the algo price at timestamp and then calculates the realizedCap per address
    addrsDF["price"] = getPrice(timestamp)
    addrsDF['realizedCapAddr']= addrsDF["balance"]*addrsDF["price"]
    # addrsDF.set_index("address", inplace=True)

    return addrsDF

def calculations(circulating_supply,realized_cap, price, connection):

    market_cap = round(circulating_supply * price, 2) # * last price
    realizedPrice = round(realized_cap / circulating_supply, 3)
    STDDevMarketCap = sql.getSTDDevMarketCap(economics_table_name, connection)

    if STDDevMarketCap != 0: #If results is not None or marketcap # of rows grater than 3 include MRV_Zscore
        mvrv_zscore = round((market_cap - realized_cap) / STDDevMarketCap , 3)
    else:  #else MRVZscore null|none
        mvrv_zscore =  None

    if realized_cap > 0: #If realized cap is 0, mrv ratio should be null
        mvrv_ratio = round(market_cap / realized_cap, 3)
    else: 
        mvrv_ratio = None
        
    return  market_cap, realizedPrice, mvrv_ratio, mvrv_zscore

def calculationsMVRV(addrsDF): #calculationsMVRVdays() uses this function,DONT DELETE
    circulatingSupply = addrsDF['balance'].sum()
    marketCap = circulatingSupply *  addrsDF["price"].iloc[-1]
    realizedCap = addrsDF['realizedCapAddr'].sum()
    MVRV_Ratio = round(marketCap / realizedCap, 6)
    
    return MVRV_Ratio

def calculationsMVRVdays(untilDate, transactionsDF, cs):
    if (untilDate > pd.to_datetime("2019-11-28", format = "%Y-%m-%d")):
        split60 = untilDate - pd.DateOffset(60, 'D')
        addrDF60o = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] <= split60])
        addrDF60y = transactionsToAdrrs(transactionsDF.loc[transactionsDF['timestamp'] > split60])
        addrDF60o = addRealizedCapColumn(addrDF60o,cs)
        addrDF60y = addRealizedCapColumn(addrDF60y,cs)
        MVRV_Ratio60o = calculationsMVRV(addrDF60o)
        MVRV_Ratio60y = calculationsMVRV(addrDF60y)

        split30= untilDate - pd.DateOffset(30, 'D')
        addrDF30o = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] <= split30])
        addrDF30y = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] > split30])
        addrDF30o = addRealizedCapColumn(addrDF30o,cs)
        addrDF30y = addRealizedCapColumn(addrDF30y,cs)
        MVRV_Ratio30o = calculationsMVRV(addrDF30o)
        MVRV_Ratio30y = calculationsMVRV(addrDF30y)


    elif (untilDate > pd.to_datetime("2019-09-29", format = "%Y-%m-%d")):
        split30= untilDate - pd.DateOffset(30, 'D')
        addrDF30o = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] <= split30])
        addrDF30y = transactionsToAdrrs( transactionsDF.loc[transactionsDF['timestamp'] > split30])
        addrDF30o = addRealizedCapColumn(addrDF30o,cs)
        addrDF30y = addRealizedCapColumn(addrDF30y,cs)
        MVRV_Ratio30o = calculationsMVRV(addrDF30o)
        MVRV_Ratio30y = calculationsMVRV(addrDF30y)

        MVRV_Ratio60o = None
        MVRV_Ratio60y = None
        
    else:
        MVRV_Ratio30o = None
        MVRV_Ratio30y = None
        MVRV_Ratio60o = None
        MVRV_Ratio60y = None

    return MVRV_Ratio30o , MVRV_Ratio30y , MVRV_Ratio60o , MVRV_Ratio60y

def calculationsNVT(tradeable_supply, market_cap, timestamp, connection):
    volume_algo = sql.get24hVolumeAlgo(timestamp,connection) / 1000000
    token_velocity = round( volume_algo / tradeable_supply, 3)
    if volume_algo > 0:
        nvt = round (market_cap/ volume_algo, 3)
    else:
        nvt = 0
    return token_velocity, nvt
    