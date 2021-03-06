from lib.configs import settings, transaction_metrics_table_name, balances_metrics_table_name
from database import insert
import signal #library to be able to quit the program with CTRL + C
import time
from sqlalchemy.exc import SQLAlchemyError
import requests
import logging

logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
f_handler = logging.FileHandler('file.log')
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
        insert.sql.startSQL() #creates engine variable | creates the tables if they dont exists.
        last_timestamp = insert.sql.getLastTimestamp(balances_metrics_table_name)

        sinceDate =  int( (last_timestamp + 3600) / 3600) * 3600
        print("START")

        while shouldNotQuit:

            try:
                print("\n******** New loop *******")
                s = time.time()

                                #----------Query Indicators  --------------------

                result = insert.insertIndicators(sinceDate)
                
                if result["status"]==200:
                    if result["type"]=="keepUpdating":
                        sinceDate += 3600
                        sleepIndicators=False #table NOT updated | succesfull insert 
                        e = time.time()
                        print("TIME indicators: " + str(e-s))

                    elif result["type"]=="transaction_not_updated":
                        print("Transaction table or Special Addresses table is not up to date")
                        e = time.time()
                        print("TIME indicators: " + str(e-s))
                        sleepIndicators=True 
                        
                    elif result["type"]=="updated":
                        print("Indicators updated")
                        e = time.time()
                        print("TIME indicators: " + str(e-s))
                        sleepIndicators=True 
                else:
                    print(result)
                    raise result
                
                #----------data updated sleep-----------
                #if sleepCS and sleepPrice and sleepIndicators: #if the three tables are updated sleep for 60seconds
                if sleepIndicators: #if the three tables are updated sleep for 60seconds
                    print(f"30s sleep:" + str({result["type"]}))
                    time.sleep(30)
                
            except requests.exceptions.RequestException as err:
                logger.warning("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate)+ " error: " + str(err))
                print("HTTP requests error\n",err,"\n!15s sleep")
                time.sleep(15) 

            # except Exception as e:
            #     raise e  

    except SQLAlchemyError as err:

        logger.exception("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate))
        print("SQLAlchemyError error\n",err,"\n!30s sleep")
        time.sleep(30)  

           
    except TimeoutError as err:
        logger.warning("lasttimestamp: " + str(last_timestamp) + " SinceTime: " + str(sinceDate) + " error: " + str(err))

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

      
 
