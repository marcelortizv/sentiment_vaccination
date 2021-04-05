import pandas as pd
import numpy as np

followers = pd.read_csv('data/followers/WHO.csv', header=None)
followerscdc = pd.read_csv('data/followers/CDCgov.csv', header=None)
list_followers = list(followers[0])
list_followerscdc = list(followerscdc[0])

list_total = list_followers +  list_followerscdc

tweets = pd.read_csv('data/vaccination_all_tweets_update04032021.csv')

id_tweets = tweets['id']
id_tweets.to_csv('data/list_tweets_information.txt', header=None, index=False)

tweets['treatment'] = tweets['id'].isin(list_total)

