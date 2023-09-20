import click 
import random
from io import BytesIO

from confluent_kafka import Consumer

import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader


c = Consumer({
    'bootstrap.servers': '13.49.128.80:19093',
    'group.id': f"{random.random()}",
    'auto.offset.reset': 'latest',
    'security.protocol': 'SSL',
    'ssl.ca.location': './auth/ca.crt',
    'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'enable.auto.commit': 'true',
    'ssl.endpoint.identification.algorithm': 'none',
})

@click.command()
@click.argument('topic')
def consume(topic: str): 
    c.subscribe(
        [topic], 
        on_assign=lambda _, p_list: print(p_list)
    )

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        with BytesIO(msg.value()) as data_io:
            reader = DataFileReader(data_io, DatumReader())
            schema = reader.meta['avro.schema'].decode('utf-8')
            schema = avro.schema.parse(schema)
            
            for record in reader:
                print(schema.fullname)
                print(record)
            
            reader.close()


consume()
