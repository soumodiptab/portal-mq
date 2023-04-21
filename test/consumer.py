from portal_client import PortalClient
class PortalConsumer:
    def __init__(self,client : PortalClient):
        
        self.client = client
        self.__register()

    def __register(self):
        resp,_=self.client.request('POST','/consumer/create',data={})
        self.client.zk.create('/consumers/{}'.format(self.client.id),ephemeral=True)
        # print(resp['message'])

    def __unregister(self):
        resp,_=self.client.request('POST','/consumer/delete',data={})
        # print(resp['message'])
    
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
        # print(resp['message'])

    def topic_lock(self,topic):
        barrier = self.client.zk.Barrier
        clock=self.client.zk.Lock('locks/topics/{}'.format(topic),self.client.id)
        clock.acquire()

    def topic_unlock(self,topic):
        clock=self.client.zk.Lock('locks/topics/{}'.format(topic),self.client.id)
        clock.release()

    def register_topic(self,topic):
        self.client.zk.create('topics/{}/'.format(topic),ephemeral=True)

    def read_message(self,topic):
        try:
            resp,status=self.client.request('POST','/read',data={"name":topic})
            if status == 200:
                return resp
        except Exception:
            pass
        return None
    

    def get_message(self,topic):
        """Blocking call that returns a message from the given topic.

        Args:
            topic (str): Topic of the message queue

        Returns:
            str: Message
        """
        if not self.is_exists_topic(topic):
            self.create_topic(topic)
        while True:
            resp_mesg = self.read_message(topic)
            if resp_mesg is None:
                self.topic_lock(topic)
                continue
            _,status=self.client.request('POST','/consume',data={"name":topic,"offset":resp_mesg['offset']})
            self.topic_unlock(topic)
            if status == 200:
                return resp_mesg['message']
    
    # def get_topics(self):
    #     resp,_=self.client.request('GET','/topics')
    #     return resp.topics
    