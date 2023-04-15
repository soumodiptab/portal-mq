from portal_client import PortalClient
class PortalProducer:
    def __init__(self,client : PortalClient):
        self.client = client
        self.register()

    def register(self):
        resp=self.client.request('POST','/producer/create',data={})
        print(resp)

    def unregister(self):
        resp=self.client.request('POST','/producer/delete',data={})
        print(resp)

    def send_message(self,topic,message:str):
        data = {
            "topic":topic,
            "message":message
        }
        resp=self.client.request('POST','/publish',data)
        print(resp)

    def __del__(self):
        self.unregister()
