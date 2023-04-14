import requests
import uuid
class MQClient:
    def __init__(self):
        pass



class Producer:
    def __init__(self, mq_client: MQClient):
        self.mq_client = mq_client
        self.id = uuid.uuid4()
    def produce(self, topic :str,message: str):

        requests.post(f'http://localhost:5000/publish', json={'message': message})
