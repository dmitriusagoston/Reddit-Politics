from dotenv import load_dotenv
import os
import praw

# set the path of the .env file
env_path = os.path.join(os.path.dirname(__file__), '../config/config.env')

# load the .env file
load_dotenv(dotenv_path=env_path)

# get the environment variables
client_id = os.getenv('client_id')
client_secret = os.getenv('client_secret')
user_agent = os.getenv('user_agent')

print(client_id)
print(client_secret)
print(user_agent)

# create a Reddit instance
reddit = praw.Reddit(
    client_id = client_id,
    client_secret= client_secret,
    user_agent= user_agent
)

# for submission in reddit.subreddit('all').hot(limit=10):
#     print(submission.title)