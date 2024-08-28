from dotenv import load_dotenv
from prawcore.exceptions import ResponseException
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql import Row
import pyspark.sql.functions as F
import os
import praw
import logging
import psycopg2

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
db_user = os.getenv('db_user')
db_pass = os.getenv('db_pass')

# create a Reddit instance
reddit = praw.Reddit(
    client_id = client_id,
    client_secret= client_secret,
    user_agent= user_agent
)

# Create a Spark session
spark = SparkSession.builder \
    .appName("Reddit Data Ingestion") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
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
    posts = []
    comments = []
    for post in subreddit.hot(limit=10):
        if not post.stickied:
            # check if post in database
            # if not, add to post_data
            if not post_exists(post.id, subreddit.display_name + '_posts'):
                post_data = Row(
                    post_id = post.id,
                    subreddit = post.subreddit.display_name,
                    title = post.title,
                    score = post.score,
                    url = post.url,
                    created_at = post.created_utc
                )
                posts.append(post_data)

                # get comments
                post.comments.replace_more(limit=0)
                for comment in post.comments.list():
                    comment_data = Row(
                        comment_id = comment.id,
                        post_id = post.id,
                        comment_body = comment.body,
                        comment_score = comment.score,
                        comment_created_at = comment.created_utc
                    )
                    comments.append(comment_data)

    return posts, comments


def save_to_database(df, table_name):
    url = "jdbc:postgresql://database.c9ok422aid0k.us-west-1.rds.amazonaws.com:5432/postgres"
    df.write \
      .format("jdbc") \
      .option("url", url) \
      .option("dbtable", table_name) \
      .option("user", db_user) \
      .option("password", db_pass) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()
        
def save_to_local(df, file_path):
    try:
        df.write.csv(file_path, mode='overwrite', header=True)
        return True
    except Exception as e:
        logging.error(e)
        return False

def post_exists(post_id, table_name):
    # check if post exists in database
    return False
    
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


def main():
    for subreddit in subreddits:
        try:
            posts, comments = fetch_subreddit_data(subreddit)
            # posts_df = spark.createDataFrame(posts)
            # comments_df = spark.createDataFrame(comments)
            # save_to_database(posts_df, subreddit.display_name + '_posts')
            # save_to_database(comments_df, subreddit.display_name + '_comments')
            drop_table(subreddit.display_name + '_posts')
            drop_table(subreddit.display_name + '_comments')  
        except ResponseException as e:
            logging.error(f'Error fetching data from subreddit: {subreddit.display_name}')
            logging.error(e)
            continue

    spark.stop()

if __name__ == '__main__':
    main()