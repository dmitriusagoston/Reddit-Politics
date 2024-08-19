from dotenv import load_dotenv
import os
import praw
import time
import logging
import csv

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

def save_data(subbreddit_name, submission_data, is_header=False):
    # save the data to the database
    filename = f'data/{subbreddit_name}_top_posts.csv'
    with open(filename, 'a') as f:
        fieldnames = ['subreddit', 'id', 'title', 'body', 'score', 'url', 
                      'num_comments', 'created_utc', 'comment_id', 'comment_body', 
                      'comment_score', 'comment_created_utc']
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if is_header:
            writer.writeheader()

        writer.writerows(submission_data)


    
        

for sub in subreddits:
    print(sub.display_name)
    # iterate through the top 100 submissions of all time
    for submission in sub.top(limit=100):
        submission.comment_sort = 'top'
        # store the comments in a list
        
 
# print(subreddit.display_name)
# print(subreddit.title)
# print(subreddit.description)
# count = 0
# while True:
#     submission = subreddit.new(limit=1).__next__()
#     print(submission.title)
#     submission.comment_sort = 'top'
#     first_comment = submission.comments[0]
#     time.sleep(10)

    # top_level_comments = list(submission.comments)
    # print(top_level_comments[1].body)