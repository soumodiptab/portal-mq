from flask import Flask, request
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe import election
import uuid
import json
import sys
import uuid
import sqlite3

app = Flask(__name__)
id = str(uuid.uuid4())
HOST = '127.0.0.1'
#PORT = 5000
PORT = sys.argv[1]
zk = KazooClient(hosts='localhost:2181')
zk.start()

zk.ensure_path('/election')
zk.ensure_path('/leader')
zk.ensure_path('/message_queue')
election_node = zk.create('/election/node-'+id, ephemeral=True)

con = sqlite3.connect(str(PORT) + ".sqlite")
cur = con.cursor()


def db_init():
    drop_consumer_table = "DROP TABLE IF EXISTS consumer;"
    drop_producer_table = "DROP TABLE IF EXISTS producer;"
    drop_topic_table = "DROP TABLE IF EXISTS topic;"

    cur.execute(drop_consumer_table)
    cur.execute(drop_producer_table)
    cur.execute(drop_topic_table)

    create_consumer_table = "CREATE TABLE consumer (id varchar(100) NOT NULL, PRIMARY KEY (id));"
    cur.execute(create_consumer_table)
    create_producer_table = "CREATE TABLE producer (id varchar(100) NOT NULL, PRIMARY KEY (id));"
    cur.execute(create_producer_table)
    create_topic_table = "CREATE TABLE topic (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(100) NOT NULL, offset INTEGER NOT NULL);"
    cur.execute(create_topic_table)


def become_leader():
    leader_loc = {'host': HOST, 'port': PORT}
    leader_loc_str = json.dumps(leader_loc)
    zk.set('/leader', leader_loc_str.encode())
    print("<Leader>")


def start_election():
    # Create an ephemeral node with a unique sequential name
    # Get the list of all ephemeral nodes under /election
    election_nodes = zk.get_children('/election')
    # Sort the nodes in ascending order
    election_nodes.sort()
    # If this node has the lowest sequence number, it becomes the leader
    if election_node == f'/election/{election_nodes[0]}':
        become_leader()


@zk.ChildrenWatch("/election")
def election_watcher(event):
    # If a node is deleted, start a new election
    start_election()


@app.route('/')
def health():
    return 'Healthy'


@app.route('/publish', methods=['POST'])
def publish_message():
    # Get the message from the request body
    message = request.json.get('message')
    # Create a new znode with the message as the data
    zk.create('/message_queue/message_',
              value=message.encode(), sequence=True)
    return 'Message published successfully.'


@app.route('/consume')
def consume_message():
    # Get the current leader of the election
    # Get a list of all messages in the queue
    messages = zk.get_children('/message_queue')
    sorted_messages = sorted(messages)
    # Get the first message in the queue
    if len(sorted_messages) > 0:
        message_path = '/message_queue/' + sorted_messages[0]
        message, _ = zk.get(message_path)
        message = message.decode()

        # Delete the message from the queue
        zk.delete(message_path)

        return message
    else:
        return 'No messages in the queue.'

# Watch for changes in the leadership
# @zk.DataWatch('/election')
# def watch_leader(data, stat):
#     global election_obj

#     # If the leader is no longer the current client, update the election object
#     if len(election_obj.contenders())>0 and election_obj.contenders()[0] != zk.client_id[1]:
#         election_obj = election.Election(zk, '/election')


@zk.ChildrenWatch("/message_queue")
def watch_children(children):
    print("Children are now: %s" % children)

# @zk.DataWatch("/my/favorite")
# def watch_node(data, stat):
#     print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


@app.route("/consumer/create", methods=['POST'])
def create_consumer():
    # Get the message from the request body
    id = request.json.get('id')

    command = "INSERT INTO consumer VALUES(" + str(id) + ");"

    cur.execute(command)
    return 'consumer created successfully.'


if __name__ == '__main__':
    start_election()
    db_init()
    app.run(host="0.0.0.0", port=PORT, debug=True, use_debugger=False,
            use_reloader=False, passthrough_errors=True)
