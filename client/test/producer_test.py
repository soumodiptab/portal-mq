from portal_client import PortalClient
from producer import PortalProducer
config=[
    {
        "host": "localhost",
        "port": 5000
    },
    {
        "host": "localhost",
        "port": 5001
    },
    {
        "host": "localhost",
        "port": 5002
    }
]
client = PortalClient(config)
producer = PortalProducer(client)

producer.send_message("test","hello world 1")
producer.send_message("test","hello world 2")
producer.send_message("test","hello world 3")

producer.send_message("test2","hello world 4")
producer.send_message("test2","hello world 5")