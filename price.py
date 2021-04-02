from lib.configs import settings, price_table_name
from database import insert
from network import interface
import signal #library to be able to quit the program with CTRL + C
import time
from sqlalchemy.exc import SQLAlchemyError
import requests
import logging
import pandas as pd

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
f_handler = logging.FileHandler('pricelog.log')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)

shouldNotQuit = True
retryNumber = 0
last_Error = None
last_timestamp = 0
sinceDate = 0

def receiveSignal(signalNumber, frame): #changes the infinite while loop to FALSE if CTRL + C is invoked to terminate the program
    global shouldNotQuit
    shouldNotQuit = False
    return
signal.signal(signal.SIGINT, receiveSignal)
signal.signal(signal.SIGILL, receiveSignal)


while shouldNotQuit: #True unless CTRL + C is invoked.
    try:
        # insert.sql.startPriceSQL() #creates engine variable | creates the tables if they dont exists.
        #last_timestamp = insert.sql.getLastTimestamp(price_table_name)
        last_timestamp = 1567296000
        print("START")

        while shouldNotQuit:

            try:
                print("\n******** New loop *******")
                s = time.time()

                                #----------Query Price Rows --------------------
                print(last_timestamp)
                priceData = interface.getPriceRows(last_timestamp)

                if priceData != None:
                    sleepIndicators = False
                    price_rows = pd.json_normalize(priceData).set_index("timestamp")
                    price_rows['pair'] = "algo-usd"
                    last_timestamp = price_rows.index.max()
                    # print(last_timestamp)
                    # exit()

                else:
                    sleepIndicators = True
                
                #----------data updated sleep-----------
                #if sleepCS and sleepPrice and sleepIndicators: #if the three tables are updated sleep for 60seconds
                if sleepIndicators: #if the three tables are updated sleep for 60seconds
                    print(f"30s sleep:" + str(last_timestamp))
                    time.sleep(30)
                
            except requests.exceptions.RequestException as err:
                logger.warning("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate)+ "error: " + str(err))
                print("HTTP requests error\n",err,"\n!15s sleep")
                time.sleep(15) 

            # except Exception as e:
            #     raise e  

    except SQLAlchemyError as err:

        logger.exception("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate))
        print("SQLAlchemyError error\n",err,"\n!30s sleep")
        time.sleep(30)  

           
    except TimeoutError as err:
        logger.warning("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate) + "error: " + str(err))

        print("TimeoutError error\n",err,"\n!30s sleep")
        time.sleep(30) 

    except Exception as err: #Major error retry in 25 seconds

        logger.exception("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate))
        print("Unkown error\n",err,"\n!")

        if last_Error == err:
            retryNumber += 0.5
        else:
            last_Error = err
            retryNumber = 1
        if retryNumber > 4.6:
            logger.error("PROGRAM ENDED lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate))
            data = '{"text": "*PROGRAM CRASHED* analytics-indexer:","attachments": [ {   "text": "%s"   }]}' %str(e)[:42]
            res = requests.post(settings["urlSlackP"], data= data, headers= {'content-type': 'application/json'})

            raise err

        time.sleep(int(10 ** retryNumber))

      
 
