from os.path import split
from lib.configs import settings, addrList, dfGenesis,dfAddresses, dfExchanges , balances_table_name, economics_table_name, balances_metrics_table_name, transaction_metrics_table_name,exceptional_addresses_table_name, exchanges_table_name
# from sqlalchemy import create_engine
import sqlalchemy
from lib.configs import settings
from network import interface
import pandas as pd

def startSQL():
    global engine

    engine = sqlalchemy.create_engine("postgres+psycopg2://{user}:{pw}@{host}:{port}/{db}"
                           .format(user=settings["database"]["user"],
                                   pw=settings["database"]["password"],
                                   db=settings["database"]["databaseName"],
                                   host=settings["database"]["host"],
                                   port=settings["database"]["port"]
                                   ))


    # dropAllTables(engine)

    createTableBalances(engine, balances_table_name)
    createTableExceptionalAddresses(engine, exceptional_addresses_table_name)
    createEconomicsTable(engine, economics_table_name)
    createBalancesMetricsTable(engine, balances_metrics_table_name)
    createTransactionMetricsTable(engine, transaction_metrics_table_name)
    

def dropAllTables(engine):
    connection = engine.connect()
    connection.execute("""DROP TABLE IF EXISTS {balances_table_name}, {economics_table_name}, {balances_metrics_table_name}, {transaction_metrics_table_name};
    """.format(balances_table_name=balances_table_name, economics_table_name = economics_table_name, balances_metrics_table_name = balances_metrics_table_name, transaction_metrics_table_name = transaction_metrics_table_name ))

    connection.close() 


def startEngineSQL():
    global engine

    engine = sqlalchemy.create_engine("postgres+psycopg2://{user}:{pw}@{host}:{port}/{db}"
                           .format(user=settings["database"]["user"],
                                   pw=settings["database"]["password"],
                                   db=settings["database"]["databaseName"],
                                   host=settings["database"]["host"],
                                   port=settings["database"]["port"]
                                   )) 



def createTableBalances(engine, balances_table_name):
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS {balances_table_name}(
                                address VARCHAR NOT NULL PRIMARY KEY,
                                index bigint NOT NULL,
                                "timestamp" bigint NOT NULL,
                                txid VARCHAR NOT NULL,
                                balance numeric(24,0) NOT NULL,
                                price numeric(8,4) NOT NULL,
                                realized_cap numeric(28,4) NOT NULL
                                );

                                CREATE INDEX IF NOT EXISTS balance_index_stats_balances ON {balances_table_name} ("balance");
                                CREATE INDEX IF NOT EXISTS balance_index_stats_timestamp ON {balances_table_name} ("timestamp");""".format(balances_table_name=balances_table_name))

    result = connection.execute("""SELECT CASE WHEN EXISTS(SELECT 1 FROM {balances_table_name}) THEN 0 ELSE 1 END AS IsEmpty;""".format(
        balances_table_name=balances_table_name))  # get max(last) timestamp of existing tables
    isEmpty = result.first()[0]

    connection.close()

    if isEmpty == 1:  # If table is empty, insert the genesis allocations
        dfGenesis.to_sql(balances_table_name, con=engine,
                         index=False, if_exists='append', chunksize=1000)


def createTableExceptionalAddresses(engine, exceptional_addresses_table_name):
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS {exceptional_addresses_table_name}(
                                address VARCHAR NOT NULL PRIMARY KEY,
                                owner VARCHAR,
                                url VARCHAR
                                ); """.format(exceptional_addresses_table_name=exceptional_addresses_table_name))

    result = connection.execute("""SELECT CASE WHEN EXISTS(SELECT 1 FROM {exceptional_addresses_table_name}) THEN 0 ELSE 1 END AS IsEmpty;""".format(
        exceptional_addresses_table_name=exceptional_addresses_table_name))  # get max(last) timestamp of existing tables
    isEmpty = result.first()[0]

    connection.close()

    if isEmpty == 1:  # If table is empty, insert the genesis allocations
        dfAddresses.to_sql(exceptional_addresses_table_name, con=engine,
                         index=False, if_exists='append', chunksize=1000)

def createTableExchanges(engine, exchanges_table_name):
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS {exchanges_table_name}(
                                address VARCHAR NOT NULL PRIMARY KEY,
                                owner VARCHAR,
                                url VARCHAR
                                ); """.format(exchanges_table_name=exchanges_table_name))

    result = connection.execute("""SELECT CASE WHEN EXISTS(SELECT 1 FROM {exchanges_table_name}) THEN 0 ELSE 1 END AS IsEmpty;""".format(
        exchanges_table_name=exchanges_table_name))  # get max(last) timestamp of existing tables
    isEmpty = result.first()[0]

    connection.close()

    if isEmpty == 1:  # If table is empty, insert the genesis allocations
        dfExchanges.to_sql(exchanges_table_name, con=engine,
                         index=False, if_exists='replace', chunksize=1000)


def createBalancesMetricsTable(engine, table_name):
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS {table_name} (
				"timestamp" bigint NOT NULL PRIMARY KEY,
                                "addresses_over_1k" bigint NOT NULL,
                                "addresses_over_10k" bigint NOT NULL,
                                "addresses_over_100k" bigint NOT NULL,
                                "addresses_over_1m" bigint NOT NULL,
                                "addresses_over_10m" bigint NOT NULL,
                                "addresses_over_100m" bigint NOT NULL,
                                "addresses_count" bigint NOT NULL,
                                "average_balance" numeric NOT NULL
                                );

                                CREATE INDEX IF NOT EXISTS timestamp_index_stats_balances_metrics ON {table_name} ("timestamp");""".format(table_name=table_name))

    connection.close()


def createTransactionMetricsTable(engine, table_name):
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS {table_name} (
                                "timestamp" bigint NOT NULL PRIMARY KEY,
                                "tps" numeric NOT NULL,
                                "tph" numeric NOT NULL,
                                "count_algo" bigint ,
                                "volume_algo" numeric,
                                "average_algo_amount" numeric,
                                "count_usdt" bigint,
                                "volume_usdt" numeric,
                                "count_usdc" bigint,
                                "volume_usdc" numeric,
                                "count_asa" bigint
                                );

                                CREATE INDEX IF NOT EXISTS timestamp_index_stats_transaction_metrics ON {table_name} ("timestamp");""".format(table_name=table_name))

    connection.close()


def createEconomicsTable(engine, table_name):
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS {table_name} (
			                    timestamp bigint NOT NULL PRIMARY KEY,
                                circulating_supply numeric NOT NULL,
                                realized_cap numeric NOT NULL,
                                market_cap numeric NOT NULL,	
                                mvrv_ratio numeric,
                                mvrv_zscore numeric,
                                realized_price numeric NOT NULL,
                                token_velocity numeric NOT NULL,
                                nvt numeric NOT NULL,
                                price numeric(8,4) NOT NULL
                                );

                            CREATE INDEX IF NOT EXISTS timestamp_index_stats_economics ON {table_name} ("timestamp");""".format(table_name=table_name))

    connection.close()


# get transaction table: since (use txIndex) | until (use day time Y-m-d)|
def AlgoTransactionsToBalances(sinceDate, untilDate, price, balances_table_name):
    with engine.begin() as connection:  # open a transaction - this runs in the
            connection.execute("""INSERT INTO {balances_table_name} SELECT * FROM 
                                            (SELECT Distinct ON (address) address , "index", timestamp, txid, balance, {price} AS price, balance * {price} AS realized_cap 
                                            FROM (SELECT "index", "timestamp", txid, sender as address, sender_balance AS balance
                                                FROM "transaction"
                                                            WHERE timestamp BETWEEN {sinceDate} AND {untilDate}-1 AND "type" = 'pay'
                                                

                                                            UNION ALL

                                                            SELECT "index", "timestamp", txid, receiver AS address, receiver_balance AS balance
                                                            FROM "transaction"
                                                            WHERE timestamp BETWEEN {sinceDate} AND {untilDate}-1 AND "type" = 'pay'
                                                            UNION ALL

                                                            SELECT "index", "timestamp", txid, "close" AS address, close_balance AS balance
                                                            FROM "transaction"
                                                            WHERE timestamp BETWEEN {sinceDate} AND {untilDate}-1 AND "type" = 'pay' AND "close" IS NOT NULL) AS tabla
                                                ORDER BY address,"index" DESC) AS S
                                            ON CONFLICT (address) DO UPDATE SET
                                                    "index" = excluded.index,
                                                    timestamp = excluded.timestamp,
                                                    txid = excluded.txid,
                                                    balance = excluded.balance,
                                                    price = excluded.price,
                                                    realized_cap = excluded.realized_Cap;
                                                    
                            WITH special_addresses AS (select * from (SELECT '737777777777777777777777777777777777777777777777777UFEJ2CI' as address, timestamp, rewards_pool_balance as balance
                                    FROM special_balances
                                    Where timestamp < {untilDate}
                                    ORDER BY timestamp DESC LIMIT 1) as rewards

                            UNION ALL

                            select * from (SELECT 'Y76M3MSY6DKBRHBL7C3NNDXGS5IIMQVQVUAB6MP4XEMMGVF2QWNPL226CA' as address, timestamp, fee_sink_balance as balance
                                    FROM special_balances
                                    Where timestamp < {untilDate}
                                    ORDER BY timestamp DESC LIMIT 1) as fees)


                            UPDATE {balances_table_name}
                            SET   	
                                    timestamp = special_addresses.timestamp,
                                    txid = 'special_address',	
                                    balance = special_addresses.balance,
                                    price = {price},
                                    realized_cap = special_addresses.balance * {price}


                            FROM   special_addresses
                            WHERE {balances_table_name}.address = special_addresses.address;
                                                    
                                                    
                                                    """.format(sinceDate=sinceDate, untilDate=untilDate, price=price, balances_table_name=balances_table_name))



def queryInsertBalancesMetrics(timestamp):
    query = """INSERT INTO {tableName} SELECT * FROM (
                                        Select {timestamp} as "timestamp",
                                        (SELECT COUNT(address) FROM {balances_table_name} WHERE "balance" > 1000000000) AS "addresses_over_1k",
                                        (SELECT COUNT(address) FROM {balances_table_name} WHERE "balance" > 10000000000) AS "addresses_over_10k",
                                        (SELECT COUNT(address) FROM {balances_table_name} WHERE "balance" > 100000000000) AS "addresses_over_100k",
                                        (SELECT COUNT(address) FROM {balances_table_name} WHERE "balance" > 1000000000000) AS "addresses_over_1m",
                                        (SELECT COUNT(address) FROM {balances_table_name} WHERE "balance" > 10000000000000) AS "addresses_over_10m",
                                        (SELECT COUNT(address) FROM {balances_table_name} WHERE "balance" > 100000000000000) AS "addresses_over_100m",
                                        (SELECT COUNT(address) FROM {balances_table_name}) AS "addresses_count",
                                        (SELECT ROUND(AVG(balance)/1000000, 2) FROM {balances_table_name} WHERE "balance" BETWEEN 1000000 AND 100000000000000) AS "average_balance") AS balances_indicators
                ON CONFLICT ("timestamp")
                DO NOTHING; 
                                        """.format(balances_table_name=balances_table_name, timestamp=timestamp, tableName=balances_metrics_table_name)
    return query


def queryInsertTransactionMetrics(sinceDate, untilDate):

    query = """  INSERT INTO {tableName} SELECT * FROM (
                                                SELECT blockchain.timestamp, ROUND("tph"::NUMERIC/3600, 2) AS tps, "tph", "count_algo", volume_algo, average_algo_amount, count_usdt, volume_usdt,  count_usdc, volume_usdc, "count_asa"
                                                FROM
                                                (
                                                SELECT {untilDate} as timestamp , COUNT("index") AS "tph" 
                                                FROM "transaction"
                                                WHERE "timestamp" BETWEEN {sinceDate} AND {untilDate}-1
                                                ) AS blockchain
                                                INNER JOIN
                                                (
                                                SELECT {untilDate} as timestamp , COUNT("index") AS "count_algo", ROUND((SUM(amount) + SUM(close_amount))/1000000, 2) AS volume_algo,  ROUND(AVG(amount)/1000000,2) AS average_algo_amount
                                                FROM "transaction"
                                                WHERE "timestamp" BETWEEN {sinceDate} AND {untilDate}-1 AND "type" = 'pay'
                                                ) AS algo ON blockchain.timestamp = algo.timestamp

                                                INNER JOIN
                                                (
                                                SELECT {untilDate} as timestamp , COUNT("index") AS "count_usdt", ROUND((SUM(amount) + SUM(close_amount))/1000000, 2) AS volume_usdt
                                                FROM "transaction"
                                                WHERE "timestamp" BETWEEN {sinceDate} AND {untilDate}-1 AND "asset_id" = 312769
                                                ) AS USDT  ON algo.timestamp = USDT.timestamp

                                                INNER JOIN
                                                (
                                                SELECT {untilDate} as timestamp , COUNT("index") AS "count_usdc", ROUND((SUM(amount) + SUM(close_amount))/1000000, 2) AS volume_usdc
                                                FROM "transaction"
                                                WHERE "timestamp" BETWEEN {sinceDate} AND {untilDate}-1 AND "asset_id" = 31566704
                                                ) AS USDC  ON USDT.timestamp = USDC.timestamp

                                                INNER JOIN
                                                (
                                                SELECT {untilDate} as timestamp , COUNT("index") AS "count_asa"
                                                FROM "transaction"
                                                WHERE "timestamp" BETWEEN {sinceDate} AND {untilDate}-1 AND "asset_id" != 0
                                                ) AS ASA  ON USDC.timestamp = ASA.timestamp 
                                        ) AS transaction_metrics

                    ON CONFLICT ("timestamp")
                    DO NOTHING;""".format(tableName=transaction_metrics_table_name, untilDate=untilDate, sinceDate=sinceDate)
    return query


def queryInsertEconomicsMetrics(economicsColumns, economicsValues):
    query = """INSERT INTO {tn} {columns} VALUES {values}
                    ON CONFLICT ("timestamp")
                    DO NOTHING;""".format(tn=economics_table_name, columns = economicsColumns, values=economicsValues)

    return query


def insertTransactionsAndBalancesMetrics( sinceDate, untilDate):
    with engine.begin() as connection:  # open a transaction - this runs in the
        connection.execute(queryInsertBalancesMetrics( untilDate))
        connection.execute(queryInsertTransactionMetrics(sinceDate, untilDate))


def insertEconomics( economicsColumns , economicsValues):
    with engine.begin() as connection:  # open a transaction - this runs in the
        connection.execute(queryInsertEconomicsMetrics(economicsColumns , economicsValues))


def getLastTimestamp(tableName):
    connection = engine.connect()
    result = connection.execute("""SELECT MAX(timestamp) FROM  "{tn}";""".format(
        tn=tableName, db=settings["database"]["databaseName"]))  # get max(last) timestamp of existing tables
    timestamp = result.first()[0]

    connection.close()

    if timestamp == 1560211200: 
        return 1560556799
    else:
        return timestamp


def getLastIndex(tableName):
    connection = engine.connect()
    result = connection.execute("""SELECT MAX("index") FROM  {tn};""".format(
        tn=tableName, db=settings["database"]["databaseName"]))  # get max(last) timestamp of existing tables
    lastIndex = result.first()[0]
    connection.close()

    if lastIndex == None:  # if tables dosent have data/rows return start date point
        return 0
    else:
        return lastIndex


def insert_df(dataFrame, tableName):
    # INSERT pandas dataframe. if it exists append the rows to the existing table
    dataFrame.to_sql(tableName, con=engine, if_exists='append')


def insert_df_replace(dataFrame, tableName):
    # INSERT pandas dataframe. if it exists replace the rows
    dataFrame.to_sql(tableName, con=engine, if_exists='replace',
                     index=False, chunksize=1000)


# get transaction table: since (use txIndex) | until (use day time Y-m-d)|
def getAlgoTransactions(sinceIndex, untilDate):

    # discard ammounts == 0 and only algo transactions (No ASA transactions)
    connection = engine.connect()

    result = pd.read_sql_query("""SELECT "index", timestamp",
                                        "txid", 
                                        "sender", "sender_balance"/1000000 AS "sender_balance", 
                                        "receiver", "to_balance"/1000000 AS "to_balance", "to_index",
                                            "amount"/1000000 AS "amount",
                                             "close","close_balance"/1000000 as "close_balance", "close_index", "close_amount"/1000000 AS "close_amount"
                                        FROM "{db}"."transactions"
                                        WHERE "index" > {since} AND "timestamp" < '{date}' AND "asset_id"= 0 AND ("amount"!=0 OR ( "amount"= 0 AND "close_amount" > 0))
                                        #LIMIT 11000;
                                        """.format(since=sinceIndex, date=str(untilDate), db=settings["database"]["databaseName"]), connection)

    connection.close()

    return result



def getTransactionsExchanges(lastCheckedIndex, amount):

    connection = engine.connect()

    result = pd.read_sql_query(""" SELECT "index", "timestamp", txid, "sender", "receiver", "amount"/1000000 AS "amount", "close", "close_amount"/1000000 AS "close_amount", "owner"
                                    FROM( 
                                                SELECT "index", "timestamp", txid, "sender", "receiver", "amount", "close", "close_amount"

                                                FROM "transaction"

                                                WHERE "index" > {index}
                                                AND ("type" = 'pay'OR ("type" = 'pay' AND "close" IS NOT NULL))
                                                AND (amount > {amount} or "close_amount" > {amount})) as trans
                                                
                                    LEFT JOIN stats_exchanges_addresses ON (trans.receiver = stats_exchanges_addresses.address)
                                    WHERE stats_exchanges_addresses.address IS NOT NULL;

                                """.format(index=lastCheckedIndex, amount = amount*1000000), connection)
    connection.close()
    return result


# get standar deviation of market cap for the MRVR ZERO Calculation
def getSTDDevMarketCap(tableName):
    connection = engine.connect()
    result = connection.execute("""SELECT  stddev("market_cap"), count("market_cap") FROM {tn}""".format(
        tn=tableName))
    row = result.first()
    connection.close()
    # if result == None or marketCap days less than 4 rows or std == 0: return None
    if not row or (row[1] < 3):
        return 0
    return float(row[0])



def getCirculatingAndRealizedCap(tableName): 
    connection = engine.connect()
    result =connection.execute("""
                                    SELECT * FROM( 
                                        select 1 as "nothing",
                                        (SELECT 10000000000 - ROUND((SUM("balance"))/1000000, 2) 
                                        FROM {balances_table}
                                        WHERE EXISTS(
                                            SELECT 1 FROM {exceptionalAdresses_table} WHERE {balances_table}.address = {exceptionalAdresses_table}.address)) AS circulating ,

                                        (SELECT ROUND((SUM("realized_cap"))/1000000 , 2) as "circulating_realized"
                                        FROM {balances_table}
                                        WHERE NOT EXISTS(
                                            SELECT 1 FROM {exceptionalAdresses_table} WHERE {balances_table}.address = {exceptionalAdresses_table}.address))  AS realized

                                    ) as " AS circulating_realized"; """.format(balances_table=tableName, exceptionalAdresses_table=exceptional_addresses_table_name))
    connection.close()

    row = result.first()

    if row:
        return float(row[1]), float(row[2])#float(row[0])
    else:
        print("** CIRCULATING SUPPLY AND realizedCap QUERY IS NULL ! **")


def getCirculating(tableName):
    connection = engine.connect()
    result = connection.execute("""SELECT SUM("balance")/1000000 as circulatingSupply  FROM {tn};
                                        """.format(tn=tableName, address_tuple=addrList))
    row = result.first()

    connection.close()

    if row:
        return float(row[0])
    else:
        print("** CIRCULATING SUPPLY AND realizedCap QUERY IS NULL ! **")


def getVolumeAlgo(timestamp):
    connection = engine.connect()
    result = connection.execute("""SELECT "volume_algo" FROM  {tn} WHERE "timestamp" = {timestamp};""".format(tn=transaction_metrics_table_name, timestamp = timestamp))  # get volume based on timestamp
    volume_algo = result.first()[0]
    connection.close()

    if volume_algo == None: 
        return 1
    else:
        return float(volume_algo)
        