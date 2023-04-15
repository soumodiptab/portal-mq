from portal_client import PortalClient
class PortalConsumer:
    def __init__(self,client : PortalClient):
        
        self.client = client
        self.__register()

    def __register(self):
        resp=self.client.request('POST','/consumer/create',data={})
        print(resp)

    def __unregister(self):
        resp=self.client.request('POST','/consumer/delete',data={})
        print(resp)
    
    def __del__(self):
        self.__unregister()

    def consume_message(self,topic):
        resp=self.client.request('POST','/consume',data={"topic":topic})
        return resp.message
    
    def get_topics(self):
        resp=self.client.request('GET','/topics')
        return resp.topics
    