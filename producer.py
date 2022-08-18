from confluent_kafka import Producer

def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n', err)
    else:
        print('%% Message delivered to %s [%d]\n',
                          (msg.topic(), msg.partition()))

def createTopic():
    print("init");
    topic = 'tuqot952-default'

    conf = {
        'bootstrap.servers': 'moped-01.srvs.cloudkafka.com:9094,moped-02.srvs.cloudkafka.com:9094,moped-03.srvs.cloudkafka.com:9094',
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
	    'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': 'tuqot952',
        'sasl.password': 'qphnYSkvj6kouaijlLr9ZlEvSj7dn57Q'
    }

    p = Producer(conf)

    try:
        p.produce(topic, "Teste de producer", callback=delivery_callback)
    except BufferError as e:
        print('%% Local producer queue is full (%d messages awaiting delivery): try again\n',
                          len(p))
    p.poll(0)

    print('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

createTopic();
