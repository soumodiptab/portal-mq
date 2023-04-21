import sys
sys.path.insert(1, '../')
import threading
import json
from portal_client import PortalClient
from consumer import PortalConsumer
import time
import random
config = json.load(open('config.json'))['brokers']
import signal

sensor = None

class CONTROLLER(threading.Thread):
    def __init__(self, topic, sleep_time=0):
        threading.Thread.__init__(self)
        self.topic = topic
        print('==============================================================')
        print('Controller topic: ', self.topic)
        print('==============================================================')
        self.sleep_time = sleep_time
        self.set_consumer()
        self._stopevent = threading.Event()

    def set_consumer(self):
        self.client = PortalClient(config)
        self.consumer = PortalConsumer(config, self.topic)

    def do_action(self, message):
        data = message.value["data"]
        print("<{}:{}> : ::\t{}\t::".format(self.ip, self.port, data))

    def run(self):
        while True:
            message = self.consumer.consume_message(self.topic)
            self.do_action(message)
            if self._stopevent.isSet():
                break

    def stop(self):
        self._stopevent.set()


def handler(signum, frame):
    print('Shutting down sensor...')
    sensor.stop()


signal.signal(signal.SIGINT, handler)

if __name__ == '__main__':
    sensor_name = sys.argv[1]
    sensor_id = sys.argv[2]
    sensor_timeout = sys.argv[3]
    if sensor_name == 'temp':
        sensor = TEMP('temp',sensor_id,sensor_timeout)
        sensor.start()
    else:
        print('Invalid sensor name')