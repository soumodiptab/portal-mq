import sys
sys.path.insert(1, '../client/')
import threading

class SENSOR(threading.Thread):
    def __init__(self, ip, port, sleep_time=5):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.topic = ip+"_"+port
        self.sleep_time = sleep_time
        self.set_producer()
        self._stopevent = threading.Event()

    def set_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=json_config_loader('config/kafka.json')['bootstrap_servers'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def get_data(self):
        pass

    def logic(self):
        self.emit()

    def emit(self):
        self.producer.send(self.topic, self.get_data())

    def flush(self, timeout=None):
        self.producer.flush(timeout=timeout)

    def timeout(self):
        time.sleep(self.sleep_time)

    def close(self):
        if self.producer:
            self.producer.close()

    def run(self):
        try:
            while not self._stopevent.isSet():
                self.logic()
                self.timeout()
            self.producer.flush()
        except Exception as e:
            print(e)
        finally:
            self.close()

    def stop(self):
        self._stopevent.set()
