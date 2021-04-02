from lib.configs import balances_table_name, economics_table_name
from database import sql
from network import interface
import time
import pandas as pd


def insertIndicators(sinceDate):

    s = time.time()
    last_timestamp_transaction_table = sql.getLastTimestamp("transaction")
    e = time.time()
    print("TIME get lasts: " + str(e-s))

    untilDate = sinceDate + 3600

    if untilDate > int(time.time()):  # if a day has passed: check for new entries
        return {"status": 200, "type": "updated"}

    # check if transaction and special_balance table are up to date
    elif untilDate > last_timestamp_transaction_table:
        return {"status": 200, "type": "transaction_not_updated"}
    else:

        print(" Since time: " + str(sinceDate) + " => " + str(interface.timestampToString(sinceDate)) +
              "   |    Until timestamp: " + str(untilDate) + " => " + str(interface.timestampToString(untilDate)))

        s = time.time()
        price = interface.getPrice(untilDate)
        e = time.time()
        print("TIME get price: " + str(e-s))

        with sql.engine.begin() as connection:
            # Upsert accounts/balances table
            s = time.time()
            connection.execute(sql.queryAlgoTransactionsToBalances(sinceDate,untilDate,price, balances_table_name))
            e = time.time()
            print("TIME AlgoTransactionsToBalances: " + str(e-s))

            # insert Transactions & Balances Metrics
            s = time.time()
            connection.execute(sql.queryInsertBalancesMetrics(untilDate))
            e = time.time()
            print("TIME insert Balances Metrics: " + str(e-s))

            s = time.time()
            connection.execute(sql.queryInsertMiscMetrics(sinceDate, untilDate))
            e = time.time()
            print("TIME insert MiscMetrics: " + str(e-s))

            s = time.time()
            connection.execute(sql.queryInsertAssetMetrics(sinceDate, untilDate))
            e = time.time()
            print("TIME insert Asset Metrics: " + str(e-s))

            # Get and calculate economics metrics

            s = time.time()
            tradeable_supply, realized_cap = sql.getTradeableAndRealizedCap(
                balances_table_name, connection)
            e = time.time()
            print("TIME 1    tradeable_supply, realized_cap: " + str(e-s))

            s = time.time()
            market_cap, realized_price, mvrv_ratio, mvrv_zscore = interface.calculations(
                tradeable_supply, realized_cap, price, connection)
            e = time.time()
            print(
                "TIME 2  market_cap, realized_price, mvrv_ratio, mvrv_zscore: " + str(e-s))

            s = time.time()
            token_velocity, nvt = interface.calculationsNVT(tradeable_supply, market_cap, untilDate, connection)
            e = time.time()
            print("TIME 3  token_velocity, nvt: " + str(e-s))

            economicsRow = {'timestamp': [untilDate], 'tradeable_supply': [tradeable_supply], 'realized_cap': [realized_cap],
                            'market_cap': [market_cap], 'mvrv_ratio': [mvrv_ratio], 'mvrv_zscore': [mvrv_zscore],
                            'realized_price': [realized_price], 'token_velocity': [token_velocity], 'nvt': [nvt], 'price': [price]}  # dictionary to convert it to a pandas dataframe

            # convert to dataframe and setting index as timestamp
            economicsRowDF = pd.DataFrame(economicsRow).set_index("timestamp")

            s = time.time()
            sql.insert_df(economicsRowDF, economics_table_name, connection)
            e = time.time()
            print("TIME insert economics: " + str(e-s))

        return {"status": 200, "type": "keepUpdating"}
