from confluent_kafka import Producer, Consumer

# Thay thế 'your_kafka_ip:your_kafka_port' bằng địa chỉ IP và cổng Kafka của bạn
bootstrap_servers = '34.30.80.246:9092'

def create_topic(topic_name):
    p = Producer({'bootstrap.servers': bootstrap_servers})
    p.produce(topic_name, key=None, value=None, headers=None)
    p.flush()

def produce_message(topic_name, message):
    p = Producer({'bootstrap.servers': bootstrap_servers})
    p.produce(topic_name, value=message)
    p.flush()

def consume_messages(topic_name):
    c = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([topic_name])
    
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: {}'.format(msg.value().decode('utf-8')))
    
    c.close()

# Tạo một topic mới
topic_name = 'your_topic_name'
create_topic(topic_name)

# Gửi một tin nhắn đến topic
message = 'Hello Kafka!'
produce_message(topic_name, message)

# Đọc tin nhắn từ topic
consume_messages(topic_name)
