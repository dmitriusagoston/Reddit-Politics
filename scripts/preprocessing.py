import pandas as pd
import numpy as np
import re
import nltk
from nltk.stem import WordNetLemmatizer

def clean_text(text):
    text = text.lower()
    text = re.sub(r"http\S+|www\S+|https\S+", '', text)  # Remove URLs
    text = re.sub(r'[^A-Za-z\s]', '', text)  # Remove special characters
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
    return text

def tokenize_text(text):
    return text.split()

def lemmatize_text(tokens):
    lemmatizer = WordNetLemmatizer()
    return [lemmatizer.lemmatize(token) for token in tokens]

def process_raw_data(file_path):
    # Load the raw data
    data = pd.read_csv(file_path)
    
    # Drop rows with missing values
    data.dropna(inplace=True)

    if 'title' in data.columns:
        data.rename(columns={'title': 'text'}, inplace=True)
        # Clean the text data
        data['cleaned_text'] = data['text'].apply(clean_text)

    elif 'comment_body' in data.columns:
        data.rename(columns={'comment_body': 'text'}, inplace=True)
        # Clean the text data
        data['cleaned_text'] = data['text'].apply(clean_text)
    
    # Tokenize the cleaned text
    data['tokens'] = data['cleaned_text'].apply(tokenize_text)
    
    # Lemmatize the tokens
    data['lemmatized_tokens'] = data['tokens'].apply(lemmatize_text)

    # save the processed data locally
    processed_file_path = file_path.replace('raw', 'processed')
    data.to_csv(processed_file_path, index=False)
    
    return data

if __name__ == '__main__':
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
        process_raw_data(f'data/raw/{table}.csv')
