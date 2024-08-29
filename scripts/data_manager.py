import psycopg2
import os
import pandas as pd
import shutil
from dotenv import load_dotenv

# set the path of the .env file
env_path = os.path.join(os.path.dirname(__file__), '../config/config.env')

# load the .env file
load_dotenv(dotenv_path=env_path)

# get the environment variables
client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
user_agent = os.getenv('user_agent')
db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')

def drop_table(table_name):
    conn = psycopg2.connect(
        host='database.c9ok422aid0k.us-west-1.rds.amazonaws.com',
        database='postgres',
        user=db_user,
        password=db_pass
    )
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    cur.close()
    conn.close()

def clear_local_data(directory):
    # Check if the directory exists
    if os.path.exists(directory):
        # Remove the directory and all its contents
        shutil.rmtree(directory)
        # Recreate the directory (optional, if you need an empty directory afterward)
        os.makedirs(directory)
    else:
        print(f"The directory {directory} does not exist.")

def fetch_all_data_and_save(table_name, file_path):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host='database.c9ok422aid0k.us-west-1.rds.amazonaws.com',
        database='postgres',
        user=db_user,
        password=db_pass
    )
    cur = conn.cursor()

    # Execute the query to fetch all data from the specified table
    cur.execute(f"SELECT * FROM {table_name}")

    # Fetch all rows from the result of the query
    rows = cur.fetchall()

    # Get the column names from the cursor
    colnames = [desc[0] for desc in cur.description]

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(rows, columns=colnames)

    # Save the DataFrame to a CSV file in the specified path
    df.to_csv(file_path, index=False)

    # Close the cursor and connection
    cur.close()
    conn.close()

    print(f"Data from {table_name} has been saved to {file_path}")

if __name__ == '__main__':
    clear_local_data('data/raw')
    tables = ['politics_posts',
              'politics_comments',
              'worldnews_posts',
              'worldnews_comments',
              'news_posts',
              'news_comments',
              'Libertarian_posts',
              'Libertarian_comments',
              'Conservative_posts',
              'Conservative_comments',
              'democrats_posts',
              'democrats_comments',
              'Republican_posts',
              'Republican_comments',
              'progressive_posts',
              'progressive_comments',
              'socialism_posts',
              'socialism_comments',
              'Liberal_posts',
              'Liberal_comments']
    for table in tables:
        fetch_all_data_and_save(table, f'data/raw/{table}.csv')
    