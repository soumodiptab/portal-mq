from kazoo.client import KazooClient
from flask import Flask, request
import json
import sys
from kazoo.client import KazooClient
from kazoo.recipe import election

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()


app = Flask(__name__)