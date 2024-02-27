import subprocess
import shutil
import redis
import pika

for id in [0, 1]:
    r = redis.Redis(port=6379 + id)
    r.flushall()
    subprocess.call('redis-cli -p {} shutdown'.format(6379 + id), shell=True)
    shutil.rmtree('./images_{}'.format(id), ignore_errors=True)

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
chan = conn.channel()
chan.queue_delete('request')
chan.queue_delete('response')
