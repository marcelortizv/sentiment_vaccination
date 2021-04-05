import sys
#from multiprocessing import Process, Queue
from multiprocessing import Queue
from threading import Thread

import tweepy
from tweepy import OAuthHandler
import os
import time

def main(output_folder, users_queue, api, num_process):
    print('process ' + str(num_process) + ' started.')
    while not users_queue.empty():
        user = users_queue.get()
        output_file = open(output_folder + '/' + user + '.csv', 'w')
        try:
            for page in tweepy.Cursor(api.followers_ids, screen_name=user, count=5000).pages():
                # print(len(page))
                for id in page:
                    output_file.write(str(id) + '\n')
        except Exception as e:
            print(e, user, num_process)

        output_file.close()

    print('Ending process:  ', num_process)


def count_missing(users_queue):
    while not users_queue.empty():
        size = users_queue.qsize()
        print('Missing users:  ', size)
        time.sleep(600)
    print('Processing tha last set of users')

if __name__ == '__main__':
    path_users_file = sys.argv[1]  # input with the users of who you want to download the followers list
    path_output_folder = sys.argv[2]  #input with the name you want to give to the folder where is gonna be the output
    path_tokens_file = sys.argv[3]


    users_file = open(path_users_file)

    users_queue = Queue()

    for user in users_file:
        users_queue.put(user.rstrip())

    #tokens_file = open('FileTokens.csv', "r")
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

    try:
        # Create target Directory
        os.mkdir(path_output_folder)
        print("Directory ", path_output_folder, " Created ")
    except FileExistsError:
        print("Directory ", path_output_folder, " already exists")

    p = Thread(target=count_missing, args=[users_queue])
    p.start()


    for i in range(num_proc):
        # print(i)
        key = tokens[i]
        consumer_key = key[0]
        consumer_secret = key[1]
        access_token = key[2]
        access_token_secret = key[3]


        try:
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            api = tweepy.API(auth, wait_on_rate_limit=True)
            # print("Successful connection!")
            p = Thread(target=main, args=[path_output_folder, users_queue, api, i + 1])
            p.start()
            processes.append(p)


        except tweepy.TweepError as err:
            print("Failed Connection! \n", err)
        except Exception as e:
            print(e)
    for p in processes:
        p.join()

    print('Download was done successfully')
