import pandas as pd
import numpy as np

followers = pd.read_csv('data/followers/WHO.csv', header=None)
followerscdc = pd.read_csv('data/followers/CDCgov.csv', header=None)
list_followers = list(followers[0])
list_followerscdc = list(followerscdc[0])

list_total = list_followers + list_followerscdc

tweets = pd.read_csv('data/vaccination_all_tweets_update04032021.csv')
tweets2 = pd.read_csv('data/tweets_vaccination2.csv')

id_tweets = tweets['id']
id_tweets.to_csv('data/list_tweets_information.txt', header=None, index=False)

screen_name = tweets2['author_screen_name']
screen_name.to_csv('data/list_users_information.txt', header=None, index=False)


tweets_df = pd.read_csv('data/tweets_vaccination2.csv')
users_df  = pd.read_csv('data/users_vaccination.csv')

all_df = pd.merge(tweets_df, users_df, left_on='author_screen_name',right_on='screen_name')
all_df.drop_duplicates(subset=['id_str'], inplace=True)

all_df['treatment'] = all_df['id_str'].isin(list_total)

