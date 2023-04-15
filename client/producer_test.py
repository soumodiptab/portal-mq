from portal_client import PortalClient
from producer import PortalProducer
from consumer import PortalConsumer
config=[
    {
        "host": "localhost",
        "port": 5000
    }
]
client = PortalClient(config)
producer = PortalProducer(client)
# producer.create_topic("test")
# producer.create_topic("test2")
producer.send_message("test","hello world 1")
producer.send_message("test","hello world 2")
producer.send_message("test","hello world 3")

producer.send_message("test2","hello world 4")
producer.send_message("test2","hello world 5")