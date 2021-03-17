from os import error
from lib.configs import balances_table_name, economics_table_name
from database import sql
from network import interface
import time
import pandas as pd


def insertIndicators(sinceDate):

    last_timestamp_transaction_table = sql.getLastTimestamp("transaction")
    last_timestamp_special_addresses_table = sql.getLastTimestamp("special_balances")

    untilDate = sinceDate + 3600

    if untilDate > int(time.time()):  #if an hour has passed: check for new entries
        return {"status":200,"type":"updated"}

    elif untilDate > last_timestamp_transaction_table or untilDate > last_timestamp_special_addresses_table: #check if transaction and special_balance table are up to date
        return {"status":200,"type":"transaction_not_updated"}
    else:

        price = interface.getPrice(untilDate)

        print(" Since time: "  + str(sinceDate) + " => " +  str(interface.timestampToString(sinceDate) )  + "   |    Until timestamp: "  + str(untilDate)   +   " => " +  str(interface.timestampToString(untilDate)))

        sql.AlgoTransactionsToBalances(sinceDate,untilDate,price, balances_table_name) #Update and insert new balances/addresses

        sql.insertTransactionsAndBalancesMetrics(sinceDate, untilDate) #insert balances and transaction metrics

        #Get and calculate economics metrics       
        try:
 
            circulating_supply, realized_cap = sql.getCirculatingAndRealizedCap(balances_table_name)

            market_cap, realized_price, mvrv_ratio, mvrv_zscore = interface.calculations(circulating_supply, realized_cap, price)

            token_velocity, nvt = interface.calculationsNVT(circulating_supply,market_cap, untilDate)
            
            economicsRow= {'timestamp' : [untilDate],'circulating_supply' : [circulating_supply], 'realized_cap' : [realized_cap] ,
            'market_cap' : [market_cap], 'mvrv_ratio' : [mvrv_ratio], 'mvrv_zscore': [mvrv_zscore],
            'realized_price': [realized_price], 'token_velocity': [token_velocity],'nvt' : [nvt],'price' :[price]} #dictionary to convert it to a pandas dataframe

            economicsRowDF = pd.DataFrame(economicsRow).set_index("timestamp")

            sql.insert_df(economicsRowDF,economics_table_name)

        except Exception as e:
            raise e



        return {"status":200,"type":"keepUpdating"}



