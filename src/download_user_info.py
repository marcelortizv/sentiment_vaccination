#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import tweepy
import time
from tweepy import OAuthHandler
from multiprocessing import Queue
from threading import Thread



def append_user_data(user, campo, line):
    '''
    :param user: the user (screen_name or user_id) who is the owner of the data
    :param campo: the field we are saving
    :param line: the string where the value is going to be added
    :return:
    '''
    try:
        if campo == "profile_image_url_https":
            line = line + user.profile_image_url_https.replace('_normal', '_200x200') + ','
        elif campo == "id" or campo == 'id_str':
            line = line + user.id_str + ','
        elif campo == "verified":
            line = line + str(user.verified) + ','
        elif campo == "followers_count":
            line = line + str(user.followers_count) + ','
        elif campo == "protected":
            line = line + str(user.protected) + ','
        elif campo == "location":
            line = line + user.location.replace(',',' ').replace('\n', '').replace('\t', '').replace('\r', '') + ','
        elif campo == "statuses_count":
            line = line + str(user.statuses_count) + ','
        elif campo == "description":
            line = line + user.description.replace(',', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ') + ','
        elif campo == "friends_count":
            line = line + str(user.friends_count) + ','
        elif campo == "screen_name":
            line = line + user.screen_name + ','
        elif campo == "favourites_count":
            line = line + str(user.favourites_count) + ','
        elif campo == "name":
            line = line + user.name.replace(',','').replace('\n', '').replace('\t', '').replace('\r', '') + ','
        elif campo == "url":
            line = line + str(user.url) + ','
        elif campo == "created_at":
            line = line + str(user.created_at) + ','
        elif campo == "listed_count":
            line = line + str(user.listed_count) + ','
        elif campo == "default_profile_image":
            line = line + str(user.default_profile_image) + ','
        elif campo == "default_profile":
            line = line + str(user.default_profile) + ','
        elif campo == 'profile_banner_url':
            try:
                line = line + user.profile_banner_url + ','
            except:
                line = line + ','
        elif campo == 'withheld_in_countries':
            try:
                line = line + str(user.withheld_in_countries).replace('[', '').replace(']', '').replace(',',' ') + ','
            except:
                line = line + ','
        else:
            line = line + 'Data not available,'
    except Exception as e:
        print(e, campo)

    return line


def process(users_queue, type, fields, api, results, num_process, processing):
    while not users_queue.empty():
        users = users_queue.get()
        processing.put(1)
        if type == "screen_name":
            try:
                user_list = api.lookup_users(screen_names=users)  # Se obtiene el objeto usuario en base al username del mismo
                for user in user_list:
                    line = ''
                    for field in fields:
                        line = append_user_data(user, field, line)
                    line = line[:-1] + '\n'
                    results.put(line)
            except Exception as e:
                print("Error:   " + str(e)+ "  in process:  " + str(num_process))


        elif type == "user_id":
            try:
                user_list = api.lookup_users(user_ids=users)  # Se obtiene el objeto usuario en base al username del mismo
                for user in user_list:
                    line = ''
                    for field in fields:
                        line = append_user_data(user, field, line)
                    line = line[:-1] + '\n'
                    results.put(line)
            except Exception as e:
                print("Error:   " + str(e)+ "  in process:  " + str(num_process))


        elif type == "id":
            try:
                user_list = api.lookup_users(ids=users)  # Se obtiene el objeto usuario en base al username del mismo
                for user in user_list:
                    line = ''
                    for field in fields:
                        line = append_user_data(user, field, line)
                    line = line[:-1] + '\n'
                    results.put(line)
            except Exception as e:
                print("Error:   " + str(e)+ "  in process:  " + str(num_process))
        x = processing.get()


def write_results(users_queue, output_file, results, processing):
    while True:
        time.sleep(5)

        while not results.empty():
            output_file.write(results.get())

        if users_queue.empty() and results.empty() and processing.empty():
            break
    output_file.close()
    print('All results written')

def count_missing(users_queue, processing):
    while True:
        size = users_queue.qsize()
        print('(aprox) Missing users:  ', 100*size)

        time.sleep(10)
        if users_queue.empty() and processing.empty():
            print('Not missing users')
            break






if __name__ == '__main__':
    path_users_file = sys.argv[1]  # input with the users of who you want to download the following list
    path_output_file = sys.argv[2]  #input with the name you want to give to the file where is gonna be the output
    config_file = sys.argv[3]  # input with the path of the configuration file
    path_tokens_file = sys.argv[4] # input with the path of the tokens files


    users_file = open(path_users_file)

    users_queue = Queue()
    results = Queue()
    processing = Queue()

    users_group = []
    cont = 0
    for user in users_file:
        if user[0] == '@':
            user = user[1:]
        users_group.append(user.rstrip())
        cont += 1
        if cont == 100:
            users_queue.put(users_group)
            users_group = []
            cont = 0
    if cont != 0:
        users_queue.put(users_group)

    conf = open(config_file, "r")
    type_id = conf.readline().rstrip()
    if type_id != 'screen_name' and type_id != 'user_id' and type_id != 'id':
        print('Wrong type of imput data:  ' + type_id)
    else:
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

        writing_process = Thread(target=write_results, args=[users_queue, output_file, results, processing])

        writing_process.start()

        p = Thread(target=count_missing, args=[users_queue, processing])
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
                p = Thread(target=process, args=[users_queue, type_id, fields, api, results, i, processing])
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
