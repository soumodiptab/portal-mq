from flask import Flask, request
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe import election
import uuid
import json
import sys
import uuid
app = Flask(__name__)
id = str(uuid.uuid4())
PORT = sys.argv[1]
#PORT = 5000
zk = KazooClient(hosts='localhost:2181')
zk.start()
election_node = zk.create('/election/node-'+id, ephemeral=True)


def become_leader():
    # Do something as the leader
    print('I am the leader!')


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


if __name__ == '__main__':
    start_election()
    app.run(host="0.0.0.0", port=PORT, debug=True, use_debugger=False,
            use_reloader=False, passthrough_errors=True)
