from mongo_connector.doc_managers.doc_manager_base import DocManagerBase


from mongo_connector import errors, constants


class DocManager(DocManagerBase):
    """DocManager that connects to MyIndexingSystem"""

    # methods will go here
    def __init__(self, url, auto_commit_interval=constants.DEFAULT_COMMIT_INTERVAL,unique_key='_id', chunk_size=constants.DEFAULT_MAX_BULK, **kwargs):
        self.auto_commit_interval = auto_commit_interval
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        print(kwargs)

    def upsert(self, doc, namespace, timestamp):
        print("INSERT :",doc)

    def update(self, document_id, update_spec, namespace, timestamp):
        print("UPDATE :",update_spec)

    def remove(self, document_id, namespace, timestamp):
        print("DELETE : ",document_id,namespace)

    def search(self, start_ts, end_ts):
        print("search from {0} to {1}".format(start_ts,end_ts))

    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        print(db,namespace)