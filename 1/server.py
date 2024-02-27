import argparse
import redis
import pika
import shutil
import os

class Callback(object):
    def __init__(self):
        self.redises = [redis.Redis(port=6379), redis.Redis(port=6380)]
    def __call__(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag = method.delivery_tag)
        body = body.decode('utf-8')
        shutil.rmtree('./response', ignore_errors=True)
        os.mkdir('./response')
        command = body.split()
        if (command[0].lower() == 'brand'):
            search = '*brand-{}*'.format(command[1])
        else:
            search = '*type-{}*'.format(command[1])
        for id in [0, 1]:
            keys = self.redises[id].keys(search)
            image_dir = './images_{}'.format(id)
            for key in keys:
                image = self.redises[id].get(key).decode('utf-8')
                shutil.copyfile('{}/{}'.format(image_dir, image), './response/{}'.format(image))
        ch.basic_publish(exchange='', routing_key='response', body='')


conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
chan = conn.channel()
chan.basic_consume('request', Callback())
chan.start_consuming()
