from portal_client import PortalClient
config = [
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
client.connect()
