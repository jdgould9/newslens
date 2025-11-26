#database_controller.py
#Jack Gould
#Stores sentiment analysis results in database

import sqlite3
import logging

def create_database_schema(connection):
    cursor = connection.cursor()

    analyzed_articles_table_creation_query = '''
    CREATE TABLE IF NOT EXISTS analyzed_articles(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id TEXT NOT NULL,
    neg REAL NOT NULL,
    neu REAL NOT NULL,
    pos REAL NOT NULL,
    compound REAL NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    url TEXT,
    author TEXT,
    published TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    job_id TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
    '''
    cursor.execute(analyzed_articles_table_creation_query)

    jobs_table_creation_query = '''
    CREATE TABLE IF NOT EXISTS jobs(
    job_id text PRIMARY KEY,
    status TEXT NOT NULL,
    keywords TEXT,
    start_date TEXT,
    end_date TEXT,
    country TEXT,
    content_type TEXT,
    news_category TEXT,
    domain TEXT,
    domain_not TEXT
    )
    '''
    cursor.execute(jobs_table_creation_query)
    connection.commit()

def initialize_database_connection(database_url):
    try:
        connection = sqlite3.connect(database_url) 
        return connection
    except sqlite3.OperationalError as e:
        print(f"Failed to connect SQL database: {e}")
        return None

def check_if_job_already_exists(connection, args):
    cursor = connection.cursor()
    
    job_selection_query = '''
    SELECT job_id, status
    FROM jobs
    WHERE job_id = :job_id
    '''
    job_selection_object = {
        'job_id' : args.job_id
    }
    
    try:
        cursor.execute(job_selection_query, job_selection_object)
    except sqlite3.ProgrammingError as e:
        print(f"Binding error: {e}")

    rows = cursor.fetchall()

    for row in rows:
        raise sqlite3.IntegrityError(f"Job {getattr(args, 'job_id')} already exists in table jobs with status {row[1]}")
    
def create_job(connection, args):
    cursor = connection.cursor()
    
    job_insertion_query = '''
    INSERT INTO jobs
    (job_id, status, keywords, start_date, end_date, country, content_type, news_category, domain, domain_not)
    VALUES(:job_id, :status, :keywords, :start_date, :end_date, :country, :content_type, :news_category, :domain, :domain_not)
    '''
    job_insertion_object = {
        'job_id' : args.job_id,
        'status' : 'PENDING',
        'keywords' : args.keywords,
        'start_date' : args.start_date,
        'end_date' : args.end_date,
        'country' : args.country,
        'content_type' : args.content_type,
        'news_category' : args.news_category,
        'domain' : args.domain,
        'domain_not' : args.domain_not
    }
    
    try:
        cursor.execute(job_insertion_query, job_insertion_object)
    except sqlite3.ProgrammingError as e:
        print(f"Binding error: {e}")
    finally:
        connection.commit()
        print(f"Successfully created {job_insertion_object['job_id']} as PENDING")

def update_job_status(connection, args, new_status):
    cursor = connection.cursor()

    status_update_query = '''
    UPDATE jobs
    SET status = :status
    WHERE job_id = :job_id
    '''
    status_update_object = {
        'job_id': args.job_id,
        'status': new_status
    }

    try:
        cursor.execute(status_update_query, status_update_object)
    except sqlite3.ProgrammingError as e:
        print(f"Binding error: {e}")
    finally:
        connection.commit()
        print(f"Successfully updated {getattr(args, 'job_id', None)} to {new_status}")

def store_analysis_results(connection, total_df, args): 
    cursor = connection.cursor()

    article_insertion_query = '''
    INSERT INTO analyzed_articles
    (article_id, neg, neu, pos, compound, title, description, url, author, published, job_id)
    VALUES(:id, :vader_neg, :vader_neu, :vader_pos, :vader_compound, :title, :description, :url, :author, :published, :job_id)
    '''
    commit_count = 0
    for index, row in total_df.iterrows():
        article_insertion_object = row.to_dict()
        article_insertion_object['job_id'] = args.job_id
        try:
            cursor.execute(article_insertion_query, article_insertion_object)
            commit_count+=1
        except sqlite3.ProgrammingError as e:
            print(f"Binding error: {e}")

    connection.commit()
    print(f"Successfully committed {commit_count} rows out of {len(total_df)} retrieved articles to database with job id {args.job_id}")
