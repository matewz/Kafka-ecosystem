from kafka import KafkaConsumer, KafkaProducer
import json
import utils as st
import time
import ast

server = st.app_settings.kafka_server


result_topic = 'NEW_PRODUCT_PROCESSED'
producer = KafkaProducer(bootstrap_servers=server)

consumer_topic = 'NEW_PRODUCT_BOUGHT'
consumer = KafkaConsumer(
    consumer_topic,
     bootstrap_servers=[server],
    )


for message in consumer:
    msg = ast.literal_eval(message.value.decode("utf-8"))
    print ("Treating product {} to {}".format(msg['product'],msg['email']))
    msg = str(msg).encode()
    producer.send(result_topic, msg)
    time.sleep(3)
