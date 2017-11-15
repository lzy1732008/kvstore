# encoding=utf-8
from __future__ import unicode_literals

import json
import requests
import threading
import time


def put_data(start):
    data = [dict(key=str(i), value=dict(column1="test"+str(i))) for i in range(start, start+100)]
    for item in data:
        requests.post('http://localhost:8500/process', data=json.dumps(item))


def get_data(num):
    for i in range(num):
        user_headers = {
            "key": str(i),
        }
        response = requests.get('http://localhost:8500/process', headers=user_headers)
        print(response.text)


def batch_put_data(start):
    batch_data = [{str(i): dict(column1="test"+str(i))} for i in range(start, start+100)]
    data_dict = dict()
    for data in batch_data:
        for key, value in data.items():
            data_dict[key] = value
    # print(json.dumps(data_dict))
    response = requests.post('http://localhost:8500/batchProcess', data=json.dumps(data_dict))
    print(response.text)


if __name__ == '__main__':
    thread_num = 4
    #print('test put data')
    #threads = [threading.Thread(target=put_data, args=(100*i,)) for i in range(thread_num)]
    #for thread in threads: thread.start()
    #for thread in threads: thread.join()
    #time.sleep(3)
    print('test batch put data')
    threads = [threading.Thread(target=batch_put_data, args=(100*(i+thread_num),)) for i in range(thread_num)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    time.sleep(3)
    print('test get data')
    threads = [threading.Thread(target=get_data, args=(100*(i+thread_num),)) for i in range(thread_num)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
