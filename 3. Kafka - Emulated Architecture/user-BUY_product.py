import time
from kafka import KafkaProducer
from json import loads
import utils as st

server = st.app_settings.kafka_server


result_topic = 'NEW_PRODUCT_BOUGHT'
list_of_buyer = [
    {'product': 'Sensor','email': 'hartmann.lorenzo@boehm.org'},
    {'product': 'Medidor de Nivel','email': 'nathanial.mueller@volkman.org'},
    {'product': 'Hidrometro','email': 'kweber@block.info'},
    {'product': 'Termometro','email': 'william.treutel@senger.net'},
    {'product': 'Tomador Pressão','email': 'dulce87@dubuque.com'}
]

for i in list_of_buyer:
    producer = KafkaProducer(bootstrap_servers=server)
    producer.send('NEW_PRODUCT_BOUGHT', str(i).encode())
    time.sleep(2)