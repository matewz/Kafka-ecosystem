import ast
import time
from kafka import KafkaConsumer, KafkaProducer
from json import loads
import utils as st

server = st.app_settings.kafka_server

consumer_topic = 'NEW_PRODUCT_FINISHED'
consumer = KafkaConsumer(
    consumer_topic,
     bootstrap_servers=[server],
    )


for message in consumer:
    msg = ast.literal_eval(message.value.decode("utf-8"))
    print ("Called logistic and sent the product {} to {}".format(msg['product'],msg['email']))
    time.sleep(3)

    
    time.sleep(3)