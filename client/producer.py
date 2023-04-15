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

    def is_exists_topic(self,topic):
        data = {
            "name":topic
        }
        resp=self.client.request('POST','/topic/exists',data)
        return resp['message']

    def create_topic(self,topic):
        data = {
            "name":topic
        }
        resp=self.client.request('POST','/topic/create',data)
        print(resp['message'])

    def send_message(self,topic,message:str):
        if not self.is_exists_topic(topic):
            self.create_topic(topic)
        data = {
            "topic":topic,
            "message":message
        }
        resp=self.client.request('POST','/publish',data)
        print(resp)

    # def __del__(self):
    #     self.unregister()
