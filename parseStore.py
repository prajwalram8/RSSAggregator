import re
import os
import shutil
import traceback
import logging
import pathlib
import feedparser  
import configparser
import pandas as pd
from datetime import datetime as dt
from datetime import date
from shared_modules.ops_utilities import add_inj_date, check_update
from shared_modules.snowflake_dataloader import orchestrate, connect_to_db, est_connection
from shared_modules.datetime_utilities import dt_to_string 
from shared_modules.email_utilities import send_mail, send_mail_update


# Configuration initialization
config = configparser.ConfigParser()
config.read('shared_modules\config.ini')

# Inititializing globals
NAME = 'GOOGLEALERTS'
SNW_USER = config['SNOWFLAKE']['USER']
SNW_ACC = config['SNOWFLAKE']['ACCOUNT']
SNW_PWD = config['SNOWFLAKE']['PASSWORD']
SNW_CHW = config['SNOWFLAKE']['WAREHOUSE']
SNW_USR = config['SNOWFLAKE']['USER_ROLE']
SNW_DB = config['SNOWFLAKE']['DATABASE']
SNW_SCH = config['SNOWFLAKE']['SCHEMA']
API_QY = config['SENDGRID']['API_KEY']

# Other Globals
STATUS = []
STATUSES = []
TEMPFILE_PATH = "TempStage"
LOGS_PATH = "AppLogs"
TEMP_STAGE_PATH = f'{NAME}_TEMP_STAGE'
INT_STAGE_PATH = os.path.join(TEMPFILE_PATH, TEMP_STAGE_PATH)
ING_STAT_FILE_NAME = f"ING_STAT_{NAME}.csv"
LOG_FILE_PATH = os.path.join(LOGS_PATH, NAME)
LOG_FILE_NAME = os.path.join(LOG_FILE_PATH, f"{dt.today().strftime('%Y_%m_%d')}.log")
INJESTION_DELIVERY_CONFIRMATION  = ['kdb081293@gmail.com']#'kdb081293@gmail.com',
LOG_FORMAT='%(asctime)s: %(name)s-%(funcName)s-%(levelname)s ==> %(message)s'
FORMATTER = logging.Formatter(LOG_FORMAT)

# Creating relevant paths
pathlib.Path(INT_STAGE_PATH).mkdir(parents=True, exist_ok=True)
pathlib.Path(LOG_FILE_PATH).mkdir(parents=True, exist_ok=True)

# Logging Initializations
logger = logging.getLogger('__main__.' + __name__)
logger.setLevel(logging.DEBUG)

# Defining a module level file handler
fh = logging.FileHandler(LOG_FILE_NAME, 'w+')  #For Logs
fh.setFormatter(FORMATTER)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

url_dict = {
    "Automotive":'https://www.google.com/alerts/feeds/03939484055531134471/15730024035608385600',
    "B2B Marketplace":  "https://www.google.com/alerts/feeds/03939484055531134471/13779756965880700658",
    "Logistics": "https://www.google.com/alerts/feeds/03939484055531134471/3982023291967526950",
    "Trade & Logistics": "https://www.google.com/alerts/feeds/03939484055531134471/6316913130448248937"
    }

def striphtml(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)

def data_preprocess(df):
    t_df = df.copy()
    t_df['id'] = t_df['id'].apply(lambda x : re.search('(?<=feed:)\d+',x).group())
    # t_df['title'] = t_df['title'].apply(lambda x : striphtml(x)) # nice to retain all the 
    t_df['published'] = t_df['published'].apply(lambda x: dt.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
    t_df['pub_date'] = t_df['published'].apply(lambda x: x.strftime("%A %d. %B %Y"))
    t_df['updated'] = t_df['updated'].apply(lambda x: dt.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
    t_df['summary'] = t_df['summary'].apply(lambda x : striphtml(x).replace("&quot;", "\""))
    t_df['domain'] = t_df['link'].apply(lambda x: re.search('(?<=url=)http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', x).group())
    t_df['direct_link'] = t_df['link'].apply(lambda x: re.search('(?<=url=)\S*(?=&ct)', x).group())
    t_df['domain_name'] = t_df['domain'].apply(lambda x: re.sub('.+\/\/|www.|\..+', '', x))

    return t_df

def main(url_dict):
    main_df = pd.DataFrame()
    for k , v in url_dict.items():
        feeds = feedparser.parse(v)['entries']
        df = pd.DataFrame(eval('feeds'))

        # Subset necessary columns
        df = df[['id','guidislink', 'link', 'title', 'published', 'updated', 'summary']].copy()

        df['alertFor'] = k

        main_df = pd.concat([df, main_df], ignore_index=True)

    main_df = data_preprocess(main_df)
    return main_df


def load_data(output):
    CSV_FILE_NAME = f"{NAME}_{date.today().strftime('%B_%d_%Y')}"
    try:
        if output.shape[0] > 0:
            output = add_inj_date(output)
            conn = connect_to_db(
                        user=SNW_USER,
                        password=SNW_PWD,
                        account=SNW_ACC,
                        session_parameter={
                            "QUERY_TAG": f"Python data scrape load - {NAME}"
                            }
                        )
            est_connection(
                            conn, 
                            database=SNW_DB,
                            compute_wh=SNW_CHW,
                            schema=SNW_SCH, 
                            user_role=SNW_USR
                            )

            STATUS = list(
                orchestrate(conn=conn,
                    df=output, 
                    table_name=f'GOOGLE_ALERTS_NEWS_STORE', 
                    database=SNW_DB,
                    csv_filename= CSV_FILE_NAME,
                    csv_file_path=INT_STAGE_PATH,
                    data_stage=f'DATA_STAGE_{NAME}')[0]
                    )
            # Update the status to include derived data points
            STATUS = check_update(STATUS,  dt_to_string(dt.today().strftime('%Y-%m-%d')), dt_to_string(dt.today().strftime('%Y-%m-%d')))
            # Append to master status list
            STATUSES.extend([STATUS])

            logger.info(f"{NAME} with {output.shape} loaded to warehouse sucessfully")

            try:
                # Update the ingestion status based on the final status recieved
                ingestion_status = pd.DataFrame(
                                data=STATUSES, 
                                columns=['stage','status', 'rows_parsed', 'rows_loaded', 'error_limit', 'errors_seen', 
                                'first_error', 'first_error_line', 'first_error_character', 'first_error_column_name', 
                                'Load_status', 'sq_start', 'sq_end']
                                )
                
                # Writing ingestion status to csv
                ingestion_status.to_csv(os.path.join(INT_STAGE_PATH, ING_STAT_FILE_NAME))
        
                if isinstance(ingestion_status, pd.DataFrame):
                    # Send Mail
                    try:
                        send_mail(
                            FILE_PATHS=[
                                os.path.join(INT_STAGE_PATH,ING_STAT_FILE_NAME), 
                                LOG_FILE_NAME
                                ], 
                            DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                            FILE_NAMES=[ING_STAT_FILE_NAME, NAME], 
                            NAME=NAME,
                            API_QY=API_QY)
                        logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
                    except:
                        logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
                    # Send Mail
            except:
                logger.warning("Injestion file needs to be checked")
                try:
                    send_mail_update(
                        FILE_PATHS=[
                            LOG_FILE_NAME
                        ],
                        FILE_NAMES=[NAME],
                        DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                        API_QY=API_QY,
                        NAME=NAME)
                    logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
                except:
                    logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")
            
    except:
        # Send mail informing that the scraping and loading activity was not successful
        logging.info("Scrapping and loading activity unsuccessful")
        try:
            send_mail_update(
                FILE_PATHS=[
                    LOG_FILE_NAME
                ],
                FILE_NAMES=[NAME],
                DEL_LIST=INJESTION_DELIVERY_CONFIRMATION, 
                API_QY=API_QY,
                NAME=NAME)
            logger.info(f"Mail to the delivery list {INJESTION_DELIVERY_CONFIRMATION} successfully tiggered")
        except:
            logger.error(f"Logs load for {NAME} into internal stage unsuccessfully Uncaught Exception: {traceback.format_exc()}")

    # Removing the temporary stage directory (INT_STAGE_PATH)
    print(ING_STAT_FILE_NAME)
    shutil.rmtree(pathlib.PurePath(INT_STAGE_PATH,ING_STAT_FILE_NAME), ignore_errors=True)
    logger.info("Local stage directory removed successfully")

    return CSV_FILE_NAME
 
if __name__ == "__main__":
    load_data(main(url_dict))