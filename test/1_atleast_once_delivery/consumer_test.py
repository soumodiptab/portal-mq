from portal_client import PortalClient
from consumer import PortalConsumer
config=[
    {
        "host": "localhost",
        "port": 5000
    }
]
client = PortalClient(config)
consumer = PortalConsumer(client)
print(consumer.consume_message("test"))
print(consumer.consume_message("test2"))
print(consumer.consume_message("test"))
print(consumer.consume_message("test2"))