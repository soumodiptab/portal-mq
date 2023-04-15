from portal_client import PortalClient
class PortalConsumer:
    def __init__(self,client : PortalClient):
        
        self.client = client
        self.__register()

    def __register(self):
        resp,_=self.client.request('POST','/consumer/create',data={})
        print(resp['message'])

    def __unregister(self):
        resp,_=self.client.request('POST','/consumer/delete',data={})
        print(resp['message'])
    
    # def __del__(self):
    #     self.__unregister()

    def is_exists_topic(self,topic):
        data = {
            "name":topic
        }
        resp,_=self.client.request('POST','/topic/exists',data)
        return resp['message']

    def create_topic(self,topic):
        data = {
            "name":topic
        }
        resp,_=self.client.request('POST','/topic/create',data)
        print(resp['message'])

    def consume_message(self,topic):
        if not self.is_exists_topic(topic):
            self.create_topic(topic)
        resp,status=self.client.request('POST','/consume',data={"name":topic})
        if status == 204:
            return None
        return resp['message']
    
    def get_topics(self):
        resp,_=self.client.request('GET','/topics')
        return resp.topics
    