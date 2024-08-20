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

os.makedirs('data', exist_ok=True)

lock = threading.Lock()

def save_data(subreddit_name, submission_data, is_header=False):
    filename = f'data/{subreddit_name}_top_posts.csv'
    with lock:
        with open(filename, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ['subreddit', 'id', 'title', 'score', 'url', 
                          'created_utc', 'comment_id', 'comment_body', 
                          'comment_score', 'comment_created_utc']
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if is_header:
                writer.writeheader()

            writer.writerows(submission_data)

def fetch_top_posts(subreddit, retries=10):
    while retries > 0:
        try:
            subreddit_obj = subreddit
            logging.info(f'Fetching subreddit: {subreddit}')

            first_run = True

            for submission in subreddit_obj.top(limit=5000):
                submission.comment_sort = 'top'
                submission.comments.replace_more(limit=1)
                submission_data = []

                for comment in submission.comments[:20]:
                    comment_data = {
                        'subreddit': subreddit,
                        'id': submission.id,
                        'title': submission.title,
                        'score': submission.score,
                        'url': submission.url,
                        'created_utc': submission.created_utc,
                        'comment_id': comment.id,
                        'comment_body': comment.body,
                        'comment_score': comment.score,
                        'comment_created_utc': comment.created_utc
                    }
                    submission_data.append(comment_data)

                if first_run:
                    save_data(subreddit, submission_data, is_header=True)
                    first_run = False
                else:
                    save_data(subreddit, submission_data)

            logging.info(f'Finished fetching subreddit: {subreddit}')
            break

        except ResponseException as e:
            if e.response.status_code == 429:
                retries -= 1
                delay = max(60 * (5 - retries), 60)  # Ensure delay is at least 60 seconds
                logging.error(f'Rate limit hit for subreddit: {subreddit} - {str(e)}')
                logging.info(f'Waiting for {delay} seconds before retrying...')
                time.sleep(delay)
            else:
                logging.error(f'ResponseException fetching subreddit: {subreddit} - {str(e)}')
                break
        except Exception as e:
            logging.error(f'Error fetching subreddit: {subreddit} - {str(e)}')
            logging.error(traceback.format_exc())
            break

def main():
    max_threads = 5  # Adjust this based on your system's capability
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(fetch_top_posts, subreddit): subreddit for subreddit in subreddits}

        for future in as_completed(futures):
            subreddit = futures[future]
            try:
                future.result()  # This will re-raise any exceptions caught during thread execution
                logging.info(f'Completed fetching subreddit: {subreddit}')
            except Exception as e:
                logging.error(f'Error in thread for subreddit {subreddit}: {str(e)}')

    logging.info('Finished fetching all subreddits')


if __name__ == '__main__':
    main()