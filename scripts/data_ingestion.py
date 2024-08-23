from dotenv import load_dotenv
from prawcore.exceptions import ResponseException
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os
import praw
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, filename='reddit_scraper.log', 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# set the path of the .env file
env_path = os.path.join(os.path.dirname(__file__), '../config/config.env')

# load the .env file
load_dotenv(dotenv_path=env_path)

# get the environment variables
client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
user_agent = os.getenv('user_agent')

# create a Reddit instance
reddit = praw.Reddit(
    client_id = client_id,
    client_secret= client_secret,
    user_agent= user_agent
)

# Create a Spark session
spark = SparkSession.builder \
    .appName("Reddit Data Ingestion") \
    .getOrCreate()

# for submission in reddit.subreddit('all').hot(limit=10):
#     print(submission.title)

subreddits = [reddit.subreddit('politics'), 
              reddit.subreddit('worldnews'), 
              reddit.subreddit('news'),
              reddit.subreddit('Libertarian'),
              reddit.subreddit('Conservative'),
              reddit.subreddit('democrats'),
              reddit.subreddit('Republican'),
              reddit.subreddit('progressive'),
              reddit.subreddit('socialism'),
              reddit.subreddit('Liberal')]

def fetch_subreddit_data(subreddit):
    post_data = []
    for post in subreddit.hot(limit=10):
        if not post.stickied:
            # check if post in database
            # if not, add to post_data
            if not post_exists(post.id):
                post_data.append({
                    'subreddit': post.subreddit.display_name,
                    'post_id': post.id,
                    'title': post.title,
                    'score': post.score,
                    'url': post.url,
                    'num_comments': post.num_comments,
                    'created_utc': post.created_utc,
                    'fetch_timestamp': current_timestamp()
                })
    
    # convert to PySpark DataFrame
    df = spark.createDataFrame(post_data)

    # save to Postgres
    save_to_database(df, table_name=subreddit.display_name)

def save_to_database(df, table_name):
    jdbc_url = "jdbc:postgresql://localhost/reddit_scraper" # replace later, along with username and password
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "password") \
        .save()
        
def post_exists(post_id, table_name):
    # check if post exists in database
    return False
    


def main():
    for subreddit in subreddits:
        try:
            fetch_subreddit_data(subreddit)
        except ResponseException as e:
            logging.error(f'Error fetching data from subreddit: {subreddit.display_name}')
            logging.error(e)
            continue

    spark.stop()

if __name__ == '__main__':
    main()