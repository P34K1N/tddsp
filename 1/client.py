import pika
import sys

def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag = method.delivery_tag)
    print('Images written in response directory')
    ch.stop_consuming()

def validate_request(line: str):
    args = line.split()
    if len(args) != 2:
        print('Expected 2 words in command, but recieved ', len(args))
        return False
    if args[0].lower() not in ['brand', 'type']:
        print('First word in command expected to be \'brand\''
              ' or \'type\', but recieved ', args[0])
        return False
    return True

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
chan = conn.channel()

for line in sys.stdin:
    line = line.strip()
    if (validate_request(line)):
        chan.basic_publish(exchange='', routing_key='request', body=line)
    chan.basic_consume('response', callback)
    chan.start_consuming()



