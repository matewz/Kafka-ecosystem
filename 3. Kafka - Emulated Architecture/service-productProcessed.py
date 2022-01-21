import ast
import time
from kafka import KafkaConsumer, KafkaProducer
from json import loads
import utils as st

server = st.app_settings.kafka_server


result_topic = 'NEW_PRODUCT_FINISHED'
producer = KafkaProducer(bootstrap_servers=server)

consumer_topic = 'NEW_PRODUCT_PROCESSED'
consumer = KafkaConsumer(
    consumer_topic,
     bootstrap_servers=[server],
    )


for message in consumer:
    msg = ast.literal_eval(message.value.decode("utf-8"))
    print ("Selected to send the product {} to {}".format(msg['product'],msg['email']))
    msg = str(msg).encode()
    producer.send(result_topic, msg)
    time.sleep(3)