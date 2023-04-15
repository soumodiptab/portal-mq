import uuid
from kazoo.client import KazooClient
import requests
import json
import logging
TIMEOUT_CONNECT = 5
TIMEOUT_REQUEST = 3
MAX_RETRY = 5
# LOG_FORMAT = '%(process)d-%(levelname)s-%(asctime)s--%(message)s'
# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
# log = logging.getLogger(__name__)
class PortalClient:
    def __init__(self,portal_connection):
        """_summary_

        Args:
            portal_connection ([array]): contains an array of portal connection objects
            Example:
            [{
                "host": "localhost",
                "port": 5000
            },{
                "host": "localhost",
                "port": 5001
            }]
        """
        # cluster of brokers to connect to
        self.portal_connection = portal_connection
        self.id = uuid.uuid4()
        self.name = "PortalClient"
        # remove later and use dynamic configuration
        self.zk = KazooClient(hosts='localhost:2181')
        self.zk.start()
        data, _ = self.zk.get('/leader')
        self.leader = json.loads(data.decode())
        
        @self.zk.DataWatch('/leader')
        def watch_leader_node(data, stat):
            # This function will be called whenever the data of the "/leader" node changes
            self.leader = json.loads(data.decode())
            print(f"New leader: {self.leader}")

    def connect(self,host,port):
        try:
            response = requests.get(f"http://{host}:{port}/",timeout=TIMEOUT_CONNECT)
            if response.status_code == 200:
                return True
            return False
        except requests.exceptions.Timeout:
            return False
        
    def test(self):
        for portal in self.portal_connection:
            if not self.connect(portal['host'],portal['port']):
                raise Exception(f"Unable to connect to broker {portal['host']}:{portal['port']}")
        
    def request(self,method,path,data,recur=0):
        if recur >= MAX_RETRY:
            raise Exception(f"Unable to connect to leader after {MAX_RETRY} retries")
        if method == 'POST':
            try :
                data["id"]=str(self.id)
                response = requests.post(f"http://{self.leader['host']}:{self.leader['port']}{path}",json=data,timeout=TIMEOUT_REQUEST)
                return response.json()
            except requests.exceptions.Timeout:
                return self.request(self,method,path,data,recur+1)
        elif method == 'GET':
            try :
                response = requests.get(f"http://{self.leader['host']}:{self.leader['port']}{path}",timeout=TIMEOUT_REQUEST)
                return response.json()
            except requests.exceptions.Timeout:
                return self.request(self,method,path,data,recur+1)
        else:
            raise Exception("Invalid method")
