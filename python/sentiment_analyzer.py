#sentiment_analyzer.py
#Jack Gould
#Fetches news data from API, performs sentiment analysis, stores results in SQL database

import sys
import argparse
import logging

import os
from dotenv import load_dotenv
from pathlib import Path

from api_client import *
from database_controller import *
from analysis import *

#Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(stream_handler)

#Environment variables
load_dotenv()
use_mock_news = False
if os.getenv('USE_MOCK_NEWS').lower() == 'true':
    use_mock_news = True
db_path = os.getenv('SQLITE_DB')
news_api_key = os.getenv('NEWS_API_KEY')
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
full_db_path = os.path.join(project_root, db_path)



#Read in parameters
parser = argparse.ArgumentParser()
parser.add_argument('--job_id', '-jid', required=True)
parser.add_argument('--keywords', '-kw')
parser.add_argument('--country', '-c', default='INT')
parser.add_argument('--start_date', '-sd')
parser.add_argument('--end_date', '-ed')
parser.add_argument('--content_type', '-ct', default='1')
parser.add_argument('--news_category', '-nc')
parser.add_argument('--domain', '-d')
parser.add_argument('--domain_not', '-dn')
args = parser.parse_args()
logger.debug(f"Read parameters: {args}")

#Initialize database connection
connection = initialize_database_connection(full_db_path)
create_database_schema(connection)
check_if_job_already_exists(connection,args)
create_job(connection, args)
try:
    #Call API client
    logger.debug("Calling API client")
    
    news_response = call_news_api(args, news_api_key, use_mock_news) 
    news = news_response.get('news')
    update_job_status(connection, args, 'PROCESSING')

    #Call analyzer script
    logger.debug("Calling analysis script")
    total_df = create_analysis_from_api_response(news)

    #Call database storage script
    logger.debug("Calling database storage script")
    store_analysis_results(connection, total_df, args) 
    update_job_status(connection, args, 'COMPLETE')
except Exception as e:
    logger.error("Sentiment analyzer failed")
    update_job_status(connection, args, 'FAILED')
    print(f"Job {args.job_id} failed: {e}")
    
connection.close()
