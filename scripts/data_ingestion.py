from dotenv import load_dotenv
from prawcore.exceptions import ResponseException
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import praw
import time
import logging
import csv
import threading
import traceback

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
              reddit.subreddit('Anarchism'),
              reddit.subreddit('Liberal'),
              reddit.subreddit('neoliberal'),
              reddit.subreddit('GreenParty'),
              reddit.subreddit('NeutralPolitics'),
              reddit.subreddit('conspiracy'),
              reddit.subreddit('PoliticalHumor'),
              reddit.subreddit('PoliticalDiscussion'),
              reddit.subreddit('atheism'),
              reddit.subreddit('Christianity')]

for subreddit in subreddits:
    print(subreddit.display_name)

def main():


if __name__ == '__main__':
    main()