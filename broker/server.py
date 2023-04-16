from flask import Flask, request, jsonify
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.recipe import election
import uuid
import sys
import uuid
import sqlite3
import json

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

# con = sqlite3.connect(str(PORT) + ".sqlite")


# Define a function to connect to the database
def get_db_connection():
    conn = sqlite3.connect(str(PORT) + ".sqlite")
    conn.row_factory = sqlite3.Row
    return conn


# Define a function to close the database connection
def close_db_connection(conn):
    conn.close()


def db_init():
    con = get_db_connection()
    cur = con.cursor()

    drop_consumer_table = "DROP TABLE IF EXISTS consumer;"
    drop_producer_table = "DROP TABLE IF EXISTS producer;"
    drop_topic_table = "DROP TABLE IF EXISTS topic;"
    drop_message_queue_table = "DROP TABLE IF EXISTS message_queue;"

    cur.execute(drop_consumer_table)
    cur.execute(drop_producer_table)
    cur.execute(drop_topic_table)
    cur.execute(drop_message_queue_table)

    create_consumer_table = "CREATE TABLE consumer (id varchar(100) NOT NULL, PRIMARY KEY (id));"
    create_producer_table = "CREATE TABLE producer (id varchar(100) NOT NULL, PRIMARY KEY (id));"
    create_topic_table = "CREATE TABLE topic (id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(100) NOT NULL UNIQUE, offset INTEGER NOT NULL, size INTEGER NOT NULL);"
    create_message_queue_table = "CREATE TABLE message_queue (id INTEGER NOT NULL, seq_no INTEGER NOT NULL, message varchar(500), PRIMARY KEY (id, seq_no), FOREIGN KEY(id) REFERENCES topic(id));"

    cur.execute(create_consumer_table)
    cur.execute(create_producer_table)
    cur.execute(create_topic_table)
    cur.execute(create_message_queue_table)

    con.commit()
    close_db_connection(con)


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
    # # Get the message from the request body
    # message = request.json.get('message')
    # # Create a new znode with the message as the data
    # zk.create('/message_queue/message_', value=message.encode(), sequence=True)
    # return 'Message published successfully.'

    name = request.json.get('name')
    message = request.json.get('message')

    con = get_db_connection()

    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")

            # Reading id, size from topic table
            command = "SELECT * FROM topic WHERE name = '" + str(name) + "';"

            cur = con.cursor()
            cur.execute(command)
            records = cur.fetchall()

            id = records[0][0]
            size = records[0][3]
            new_size = size+1
            

            # Update the size of the existing queue in topic table
            update_command = "UPDATE topic SET size = " + str(new_size) + " WHERE name = '" + str(name) + "';"
            cur.execute(update_command)
            
            # Write in the message_queue table
            seq_no = size+1
            write_command = "INSERT INTO message_queue VALUES(" + str(id) + ", " + str(seq_no) + ", '" + str(message) + "');"
            cur.execute(write_command)

            # Commit the transaction
            con.execute("COMMIT")
            
        return jsonify({'message': 'published in topic successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'error publishing message'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


@app.route('/consume', methods=['POST'])
def consume_message():
    # # Get the current leader of the election
    # # Get a list of all messages in the queue
    # messages = zk.get_children('/message_queue')
    # sorted_messages = sorted(messages)
    # # Get the first message in the queue
    # if len(sorted_messages) > 0:
    #     message_path = '/message_queue/' + sorted_messages[0]
    #     message, _ = zk.get(message_path)
    #     message = message.decode()

    #     # Delete the message from the queue
    #     zk.delete(message_path)

    #     return message
    # else:
    #     return 'No messages in the queue.'

    name = request.json.get('name')
    con = get_db_connection()

    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")

            # Reading id, size from topic table
            command = "SELECT * FROM topic WHERE name = '" + str(name) + "';"
            cur = con.cursor()
            cur.execute(command)
            records = cur.fetchall()

            id = records[0][0]
            offset = records[0][2]
            size = records[0][3]

            if(offset < size):
                # Fetch from message_queue table
                fetch_command = "SELECT * FROM message_queue WHERE id = " + str(id) + " AND seq_no = " + str(offset+1) + ";"
                cur.execute(fetch_command)
                records = cur.fetchall()
                message = records[0][2]

                # Update topic table, increase offset
                update_command = "UPDATE topic SET offset = " + str(offset+1) + " WHERE name = '" + str(name) + "';"
                cur.execute(update_command)
                return jsonify({'message' : message})
            else:
                return jsonify({'message': ''}), 204
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'error consuming message'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


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

    con = get_db_connection()
    
    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            command = "INSERT INTO consumer VALUES(" + str(id) + ");"
            cur = con.cursor()
            cur.execute(command)

            # Commit the transaction
            con.execute("COMMIT")
        return jsonify({'message': 'consumer created successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'consumer created unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


@app.route("/consumer/delete", methods=['POST'])
def delete_consumer():
    # Get the message from the request body
    id = request.json.get('id')

    con = get_db_connection()

    # DELETE FROM artists_backup WHERE artistid = 1;
    
    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            command = "DELETE FROM consumer WHERE id = " + str(id) + ";"
            cur = con.cursor()
            cur.execute(command)
            # Commit the transaction
            con.execute("COMMIT")
        return jsonify({'message': 'consumer deleted successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'consumer deleted unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


@app.route("/producer/create", methods=['POST'])
def create_producer():
    # Get the message from the request body
    id = request.json.get('id')

    con = get_db_connection()
    
    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            command = "INSERT INTO producer VALUES(" + str(id) + ");"
            cur = con.cursor()
            cur.execute(command)
            # Commit the transaction
            con.execute("COMMIT")
        return jsonify({'message': 'producer created successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'producer creation unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


@app.route("/producer/delete", methods=['POST'])
def delete_producer():
    # Get the message from the request body
    id = request.json.get('id')

    con = get_db_connection()

    # DELETE FROM artists_backup WHERE artistid = 1;

    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            command = "DELETE FROM producer WHERE id = " + str(id) + ";"
            cur = con.cursor()
            cur.execute(command)
            # Commit the transaction
            con.execute("COMMIT")
        return jsonify({'message': 'producer deleted successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'producer deletion unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


@app.route("/topic/exists", methods=['POST'])
def exists_topic():
    # Get the message from the request body
    name = request.json.get('name')

    con = get_db_connection()
    
    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            command = "SELECT * FROM topic WHERE name = '" + str(name) + "';"
            print(command)
            cur = con.cursor()
            cur.execute(command)
            rows = cur.fetchall()
            print("len: " + str(len(rows)))

            # Commit the transaction
            con.execute("COMMIT")

        if(len(rows) > 0):
            return jsonify({'message': True})
        else:
            return jsonify({'message': False})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'checking unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)

@app.route("/topic/create", methods=['POST'])
def create_topic():
    # Get the message from the request body
    name = request.json.get('name')
    # offset = request.json.get('offset')

    con = get_db_connection()
    
    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")

            command = "INSERT INTO topic (name, offset, size) VALUES('" + str(name) + "', 0, 0)" + ";"
            print(command)
            cur = con.cursor()
            cur.execute(command)
            # Commit the transaction
            con.execute("COMMIT")
        return jsonify({'message': 'topic created successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'topic creation unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)


@app.route("/topic/delete", methods=['POST'])
def delete_topic():
    # Get the message from the request body
    name = request.json.get('name')

    con = get_db_connection()

    # DELETE FROM artists_backup WHERE artistid = 1;

    try:
        with con:
            # Start a transaction
            con.execute("BEGIN")
            command = "DELETE FROM topic WHERE name = '" + str(name) + "';"

            cur = con.cursor()
            cur.execute(command)
            # Commit the transaction
            con.execute("COMMIT")
        return jsonify({'message': 'topic deleted successfully'})
    except:
        # Rollback the transaction if there was an error
        con.execute("ROLLBACK")
        return jsonify({'message': 'topic deletion unsuccessful!!'}), 501
    finally:
        print("closing db connection")
        close_db_connection(con)
        

if __name__ == '__main__':
    start_election()
    db_init()
    app.run(host="0.0.0.0", port=PORT, debug=True, use_debugger=False,
            use_reloader=False, passthrough_errors=True)
