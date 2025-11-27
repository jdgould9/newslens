#database_controller.py
#Jack Gould
#Stores sentiment analysis results in database

import psycopg2

def initialize_database_connection(db_credentials):
    #create_database_if_needed()
    try:
        news_analysis_connection = psycopg2.connect(
            host=db_credentials[0],
            dbname=db_credentials[1],
            user=db_credentials[2],
            password=db_credentials[3],
            port=db_credentials[4],
        )
        news_analysis_connection.autocommit=False
        return news_analysis_connection
    except psycopg2.OperationalError as e:
        print(f"Couldn't connect to database: {e}")
        return None

def create_job(connection, args): 
    job_insertion_query = '''
    INSERT INTO jobs
    (job_id, status, keywords, start_date, end_date, country, content_type, news_category, domain_yes, domain_not)
    VALUES(%(job_i)s, %(status)s, %(keywords)s, %(start_date)s, %(end_date)s, %(country)s, %(content_type)s, %(news_category)s, %(domain_yes)s, %(domain_not)s)
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
        'domain_yes' : args.domain_yes,
        'domain_not' : args.domain_not
    }
    with connection.cursor() as cursor:
        cursor.execute(job_insertion_query, job_insertion_object)
        
    connection.commit()
    print(f"Successfully created {job_insertion_object['job_id']} as PENDING")

def update_job_status(connection, args, new_status):
    status_update_query = '''
    UPDATE jobs
    SET status = %(status)s
    WHERE job_id = %(job_id)s
    '''
    status_update_object = {
        'job_id': args.job_id,
        'status': new_status
    }

    with connection.cursor() as cursor:
        cursor.execute(status_update_query, status_update_object)
    connection.commit()
    print(f"Successfully updated {getattr(args, 'job_id', None)} to {new_status}")

def store_analysis_results(connection, total_df, args): 
    article_insertion_query = '''
    INSERT INTO analyzed_articles
    (article_id, neg, neu, pos, compound, title, description, url, author, published, job_id)
    VALUES(%(id)s, %(vader_neg)s, %(vader_neu)s, %(vader_pos)s, %(vader_compound)s, %(title)s, %(description)s, %(url)s, %(author)s, %(published)s, %(job_id)s)
    '''
    commit_count = 0
    with connection.cursor() as cursor:
        for index, row in total_df.iterrows():
            article_insertion_object = row.to_dict()
            article_insertion_object['job_id'] = args.job_id
            cursor.execute(article_insertion_query, article_insertion_object)
            commit_count+=1

    connection.commit()
    print(f"Successfully committed {commit_count} rows out of {len(total_df)} retrieved articles to database with job id {args.job_id}")

###UNUSED
def check_if_job_already_exists(connection, args):
    job_selection_query = '''
    SELECT job_id, status
    FROM jobs
    WHERE job_id = %(job_id)s
    '''
    job_selection_object = {
        'job_id' : args.job_id
    }

    with connection.cursor() as cursor:
        cursor.execute(job_selection_query, job_selection_object)
        row = cursor.fetchone()
        if row:
            pass  

def create_database_if_needed():
    postgres_connection = psycopg2.connect(
        host='localhost',
        dbname='postgres',
        user='postgres',
        password='mysupersecretpassword!!!!', port=5432
    )
    
    cursor = postgres_connection.cursor()
    db_exists_query = '''
    SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'news_analysis');
    '''
    cursor.execute(db_exists_query)
    exists = cursor.fetchone()[0]
    if not exists:
        create_database_schema(postgres_connection)

    postgres_connection.close()

def create_database_schema(connection):
    connection.autocommit=True
    cursor = connection.cursor()

    db_create_query = '''
    CREATE DATABASE news_analysis;
    '''

    jobs_table_creation_query = '''
    CREATE TABLE IF NOT EXISTS jobs(
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    keywords TEXT,
    start_date TEXT,
    end_date TEXT,
    country TEXT,
    content_type TEXT,
    news_category TEXT,
    domain_yes TEXT,
    domain_not TEXT
    )
    '''
    cursor.execute(jobs_table_creation_query)

    analyzed_articles_table_creation_query = '''
    CREATE TABLE IF NOT EXISTS analyzed_articles(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    job_id TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
    )
    '''
    cursor.execute(analyzed_articles_table_creation_query)
    
    connection.commit()
