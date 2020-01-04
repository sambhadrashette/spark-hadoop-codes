from json import loads

from kafka import KafkaConsumer

consumer = KafkaConsumer('iot', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         value_deserializer=lambda m: loads(m.decode('utf-8')))

for message in consumer:
    message = message.value
    print('received : {}'.format(message))
