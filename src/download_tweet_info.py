#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import tweepy
import time
from tweepy import OAuthHandler
from multiprocessing import Queue
from threading import Thread



def append_tweet_data(tweet, campo, line):
    '''
    :param user: the user (screen_name or user_id) who is the owner of the data
    :param campo: the field we are saving
    :param line: the string where the value is going to be added
    :return:
    '''
    try:
        if campo == "created_at":
            line = line + str(tweet.created_at)+ ','
        elif campo == "id" or campo == 'id_str':
            line = line + tweet.id_str + ','
        elif campo == "text":
            try:
                line = line + '"' + tweet.retweeted_status.full_text.replace('\n',' ').replace('"', '\'',) + '",'
            except:
                line = line + '"' + tweet.full_text.replace('\n',' ').replace('"', '\'',) + '",'
        elif campo == "source":
            line = line + '"' + tweet.source + '",'
        elif campo == "in_reply_to_status_id" or campo == 'in_reply_to_status_id_str':
            try:
                line = line + tweet.in_reply_to_status_id_str + ','
            except:
                line = line + ','
        elif campo == "in_reply_to_user_id" or campo == 'in_reply_to_user_id_str':
            try:
                line = line + tweet.in_reply_to_user_id_str + ','
            except:
                line = line + ','
        elif campo == "in_reply_to_screen_name":
            try:
                line = line + tweet.in_reply_to_screen_name + ','
            except:
                line = line + ','
        elif campo == "author_name":
            line = line + '"' + tweet.user.name.replace('\n',' ').replace('"', '\'',) + '",'
        elif campo == "author_location":
            line = line + '"' + tweet.user.location.replace('\n',' ').replace('"', '\'',) + '",'
        elif campo == "author_description":
            line = line + '"' + tweet.user.description.replace('\n',' ').replace('"', '\'',) + '",'
        elif campo == "author_screen_name":
            line = line + tweet.user.screen_name + ','
        elif campo == "author_followers":
            line = line + str(tweet.user.followers_count) + ','
        elif campo == "author_followings":
            line = line + str(tweet.user.friends_count) + ','
        elif campo == "coordinates":
            try:
                coordinates = '"' + str(tweet.coordinates['coordinates'][1]) + ',' + str(
                    tweet.coordinates['coordinates'][0]) + '"'
            except:
                coordinates = ''
            line = line + coordinates + ','
        elif campo == "type":
            type = "TW"
            try:
                tmp = tweet.retweeted_status
                type = "RT"
            except:
                pass
            try:
                tmp = tweet.quoted_status
                type = "QT"
            except:
                pass
            try:
                tmp = tweet.in_reply_to_screen_name
                if tmp != None and type == "TW":
                    type = "RP"
            except:
                pass
            line = line + type + ','
        elif campo == "user_retweeted":
            try:
                tmp = tweet.retweeted_status
                user_retweeted = tmp.user.screen_name
            except:
                user_retweeted = ''
            line = line + user_retweeted + ','
        elif campo == "user_quoted":
            try:
                tmp = tweet.quoted_status
                user_quoted = tmp.user.screen_name
            except:
                user_quoted = ''
            line = line + user_quoted + ','
        elif campo == "user_replied":
            try:
                user_replied = tweet.in_reply_to_screen_name
                line = line + user_replied + ','
            except:
                user_replied = ''
                line = line + user_replied + ','
        elif campo == "content_quoted":
            try:
                tmp = tweet.quoted_status
                content_quoted = tmp.full_text.replace('\n',' ').replace('"', '\'',)
            except:
                content_quoted = ''
            line = line + '"' + content_quoted + '",'
        elif campo == "retweet_count":
            line = line + str(tweet.retweet_count) + ','
        elif campo == "favorite_count":
            line = line + str(tweet.favorite_count) + ','
        elif campo == "lang":
            line = line + tweet.lang + ','
        elif campo == "url":
            url = 'https://twitter.com/' + tweet.user.screen_name + '/' + 'status' + '/' + tweet.id_str
            line = line + url + ','
        elif campo == "photo_uploaded":
            try:
                media = tweet.entities['media']
                if media[0]['expanded_url'].split('/')[-2] == 'photo':
                    photo_link = media[0]['expanded_url']
                else:
                    photo_link = ''
            except:
                photo_link = ''
                pass
            line = line + photo_link + ','
        elif campo == "video_uploaded":
            try:
                media = tweet.entities['media']
                if media[0]['expanded_url'].split('/')[-2] == 'video':
                    video_link = media[0]['expanded_url']
                else:
                    video_link = ''
            except:
                video_link = ''
                pass
            line = line + video_link + ','
        elif campo == "youtube_video":
            youtube = ''
            if len(tweet.entities['urls']) > 0:
                url = tweet.entities['urls'][0]
                if 'youtu.be' in url['expanded_url'] or 'youtube.com' in url['expanded_url']:
                    youtube = url['expanded_url']
            line = line + '"' + youtube + '",'

        elif campo == "shared_link":
            link = ''
            if len(tweet.entities['urls']) > 0:
                url = tweet.entities['urls'][0]
                link = url['expanded_url']

            line = line + '"' + link + '",'
        elif campo == "mentions":
            mentions_list = ''
            mentions = tweet.entities['user_mentions']

            for mention in mentions:
                mentions_list += mention['screen_name'].lower() + ';'

            mentions_list = mentions_list[:-1]
            line = line + '"' + mentions_list + '",'
        elif campo == "hashtags":
            hashtags_list = ''
            hashtags = tweet.entities['hashtags']

            for hashtag in hashtags:
                hashtags_list += hashtag['text'].lower() + ';'

            hashtags_list = hashtags_list[:-1]
            line = line + '"' + hashtags_list + '",'

    except Exception as e:
        print(e, campo)

    return line


def process(posts_queue, fields, api, results, num_process, processing):
    while not posts_queue.empty():
        posts = posts_queue.get()
        processing.put(1)
        try:
            user_list = api.statuses_lookup(posts, tweet_mode='extended')  # Se obtiene el objeto usuario en base al username del mismo
            for user in user_list:
                line = ''
                for field in fields:
                    line = append_tweet_data(user, field, line)
                line = line[:-1] + '\n'
                results.put(line)
        except Exception as e:
            print("Error:   " + str(e)+ "  in process:  " + str(num_process))


        x = processing.get()


def write_results(posts_queue, output_file, results, processing):
    while True:
        time.sleep(5)

        while not results.empty():
            output_file.write(results.get())

        if posts_queue.empty() and results.empty() and processing.empty():
            break
    output_file.close()
    print('All results written')

def count_missing(posts_queue, processing):
    while True:
        size = posts_queue.qsize()
        print('(aprox) Missing users:  ', 100*size)

        time.sleep(10)
        if posts_queue.empty() and processing.empty():
            print('Not missing users')
            break






if __name__ == '__main__':
    path_posts_file = sys.argv[1]  # input with the links or ids of the tweets you want to download the data
    path_output_file = sys.argv[2]  #input with the name you want to give to the file where is gonna be the output
    config_file = sys.argv[3]  # input with the path of the configuration file
    path_tokens_file = sys.argv[4]  # input with the path of the tokens files

    posts_file = open(path_posts_file)

    posts_queue = Queue()
    results = Queue()
    processing = Queue()

    posts_group = []
    cont = 0
    for post in posts_file:
        post_id = post.split('/')[-1]
        posts_group.append(post_id.rstrip())
        cont += 1
        if cont == 100:
            posts_queue.put(posts_group)
            posts_group = []
            cont = 0
    if cont != 0:
        posts_queue.put(posts_group)

    conf = open(config_file, "r")


    first_line = conf.readline().rstrip()
    fields = first_line.split(',')

    tokens_file = open(path_tokens_file, "r")
    # fields = fields[:-1]
    # print(fields)

    tokens_file.readline()
    tokens = []

    for line in tokens_file:
        line = line.rstrip()
        claves = line.split(',')
        tokens.append(claves)


    num_proc = len(tokens)

    processes = []

    output_file = open(path_output_file, 'w')
    output_file.write(first_line + '\n')

    writing_process = Thread(target=write_results, args=[posts_queue, output_file, results, processing])

    writing_process.start()

    p = Thread(target=count_missing, args=[posts_queue, processing])
    p.start()


    for i in range(num_proc):
        # print(i)
        key = tokens[i]
        consumer_key = key[0]
        consumer_secret = key[1]
        access_token = key[2]
        access_token_secret = key[3]


        ###  It's missing the fields  !!!!!!!!!!!!!!!!!!!!!

        try:
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            api = tweepy.API(auth, wait_on_rate_limit=True)
            # print("Successful connection!")
            p = Thread(target=process, args=[posts_queue, fields, api, results, i, processing])
            p.start()
            processes.append(p)


        except tweepy.TweepError as err:
            print("Failed Connection! \n", err)
        except Exception as e:
            print(e)
    for p in processes:
        p.join()

    print('Download was done successfully')

    writing_process.join()
