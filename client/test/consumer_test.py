from portal_client import PortalClient
from consumer import PortalConsumer
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
producer = PortalConsumer(client)