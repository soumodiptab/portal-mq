import uuid
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
        self.portal_connection = portal_connection
        self.id = uuid.uuid4()
        self.name = "PortalClient"

    def connect(self):
        pass
    
    def get_leader(self):

    def request(path,method):
