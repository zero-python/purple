# -*- coding:utf-8 -*-
import pika
import configparser


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


config = configparser.ConfigParser()
config.read('rabbitmq.ini')
credentials = pika.PlainCredentials(username=config['rabbitmq']['username'],
                                    password=config['rabbitmq']['password'])
connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['rabbitmq']['host'],
                                                               port=int(config['rabbitmq']['port']),
                                                               virtual_host=config['rabbitmq']['virtual_host'],
                                                               credentials=credentials))
channel = connection.channel()
channel.basic_consume(queue=config['rabbitmq']['queue'], on_message_callback=callback, auto_ack=True)
print(" [*] Waiting for messages. To exit press CTRL+C")


if __name__ == '__main__':
    channel.start_consuming()
