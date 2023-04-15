from portal_client import PortalClient
class PortalConsumer:
    def __init__(self,client : PortalClient):
        self.client = client
        self.register()

    def register(self):
        resp=self.client.request('POST','/consumer/create',data={})
        print(resp)

    def unregister(self):
        resp=self.client.request('POST','/consumer/delete',data={})
        print(resp)
    
    def __del__(self):
        self.unregister()

    def consume_message(self,topic):
        resp=self.client.request('POST','/consume')
        print(resp)