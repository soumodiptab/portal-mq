from portal_client import PortalClient
class PortalConsumer:
    def __init__(self,client : PortalClient):
        
        self.client = client
        self.__register()

    def __register(self):
        resp=self.client.request('POST','/consumer/create',data={})
        print(resp['message'])

    def __unregister(self):
        resp=self.client.request('POST','/consumer/delete',data={})
        print(resp['message'])
    
    # def __del__(self):
    #     self.__unregister()

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

    def consume_message(self,topic):
        if not self.is_exists_topic(topic):
            self.create_topic(topic)

        resp=self.client.request('POST','/consume',data={"topic":topic})
        return resp.message
    
    def get_topics(self):
        resp=self.client.request('GET','/topics')
        return resp.topics
    