import random
from datetime import datetime
from json import dumps
from time import sleep

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    data = {}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sensor = 'LDR'
    reading = random.randint(0, 80)
    data['time'] = now
    data['sensor'] = sensor
    data['reading'] = reading
    producer.send('iot', value=data)
    print('sent : {}'.format(data))
    sleep(1)
