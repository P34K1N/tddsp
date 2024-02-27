import os
import subprocess
import shutil
import datetime
import pika

def init_redis(id):
    subprocess.call('redis-server --port {} --daemonize yes'.format(6379 + id), shell=True)
    image_dir = './images_{}'.format(id)
    if os.path.exists(image_dir):
        shutil.rmtree(image_dir, ignore_errors=True)
    os.mkdir(image_dir)

for id in [0, 1]:
    init_redis(id)

start = datetime.datetime.now()
subprocess.call('python3 ./etl.py', shell=True)
print('Data loaded in', datetime.datetime.now() - start)

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
chan = conn.channel()
chan.queue_declare('request')
chan.queue_declare('response')

subprocess.Popen('python3 ./server.py', shell=True)
