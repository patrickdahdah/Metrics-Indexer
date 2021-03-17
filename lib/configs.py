import json
import os
import argparse
from numpy.core.getlimits import MachArLike
import pandas as pd

balances_table_name = "stats_balances"
economics_table_name = "stats_economics"
balances_metrics_table_name = "stats_balances_metrics"
transaction_metrics_table_name = "stats_transaction_metrics"
exceptional_addresses_table_name = "stats_exceptional_addresses"
exchanges_table_name = "stats_exchanges_addresses"

def getSettings():
    global settings
    try:
        settings
    except NameError:
        
        # Initialize parser 
        parser = argparse.ArgumentParser() 
        # Adding optional argument 
        parser.add_argument("-s", "--Settings", help = "Add settings path. example: $ python3 -s /home/blabla/settingsT.json") 
        # Read arguments from command line 
        args = parser.parse_args() 

        

        if args.Settings: 
            print("Path as: % s" % args.Settings)
            with open(args.Settings) as f:
                settings = json.load(f)

        elif 'ANALYTICS_SETTINGS' in os.environ:
            ruta=os.getenv('ANALYTICS_SETTINGS')
            try: 
                with open(ruta) as f:
                    settings = json.load(f)
            except Exception as e:
                print(str(e) + "\n\Environment settings variable name exists but no such file or something went wrong!\n")

        else:
            basepath = os.path.dirname(__file__)
            filepath = os.path.abspath(os.path.join(basepath, "..", "settingsT.json"))
            with open(filepath) as f:
                settings = json.load(f)
          
getSettings()


dfGenesis = pd.read_csv("data/genesis.csv", index_col=False ) #dataframe with genesis allocations

dfAddresses = pd.read_csv('data/Addresses.csv', index_col=False ) #list of inc/foundation addresses
addrList = tuple(dfAddresses['address'].tolist())


dfExchanges = pd.read_csv("data/exchanges.csv", index_col=False ) #list of exchange's addresses 

