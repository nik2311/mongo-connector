from mongo_connector.doc_managers.doc_manager_base import DocManagerBase


from mongo_connector import errors, constants
import os
import functools
import re
import logging
import json
import importlib

LOG = logging.getLogger(__name__)

class ElasticFeeder(object):


    def read_config(self):
        conf_path=os.environ.get('ELASTIC_CONF',None)
        if conf_path:
            with open(conf_path) as f:
                data = json.load(f)
            return data

    def __init__(self):
        self.config = self.read_config()

    def upsert(self, doc, namespace, timestamp):
        print("INSERT :",doc,namespace)

    def update(self, document_id, update_spec, namespace, timestamp):
        print("UPDATE :",update_spec,namespace)

    def remove(self, document_id, namespace, timestamp):
        print("DELETE : ",document_id,namespace)


class DocManager(DocManagerBase):
    """DocManager that connects to MyIndexingSystem"""

    # methods will go here
    def __init__(self, url, auto_commit_interval=constants.DEFAULT_COMMIT_INTERVAL,unique_key='_id', chunk_size=constants.DEFAULT_MAX_BULK, **kwargs):
        self.auto_commit_interval = auto_commit_interval
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        custom_handler = os.environ.get('CustomHandlerModule',None)
        custome_handler_class = os.environ.get('CustomHandlerClass',None)
        if custom_handler and custome_handler_class:
            module = importlib.import_module(custom_handler)
            custome_handler_class= getattr(module, custome_handler_class)
            self.es_connect = custome_handler_class()
        else:
            self.es_connect = ElasticFeeder()
        print(kwargs)

    def upsert(self, doc, namespace, timestamp):
        # print("INSERT :",doc,namespace)
        self.es_connect.upsert(doc, namespace, timestamp)

    def update(self, document_id, update_spec, namespace, timestamp):
        # print("UPDATE :",update_spec,namespace)
        self.es_connect.update(document_id, update_spec, namespace, timestamp)

    def remove(self, document_id, namespace, timestamp):
        print("DELETE : ",document_id,namespace)

    def search(self, start_ts, end_ts):
        pass
        # print("search from {0} to {1}".format(start_ts,end_ts))

    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        pass
        # print(db,namespace)