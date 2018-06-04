from mongo_connector.doc_managers.doc_manager_base import DocManagerBase


from mongo_connector import errors, constants
from neo4j.v1 import GraphDatabase



class Neo4jconnector(object):
    
    def __init__(self,uri="bolt://localhost:7687",auth=("neo4j", "neodev")):
        self.driver = GraphDatabase.driver(uri, auth=auth
    


    def createUser(self,user):
        usr = dict()
        usr['appFBId'] = user['appFBId']
        usr['fullName'] = user["fullName"]
        usr['mobiles']=user['mobileNumber']
        usr['email']=user.get('email',None)
        usr['legitId']=user.get('legit_id',None)
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                tx.run("Create (u:UserNode {appFBId:{appFBId},fullName:{fullName},mobile:{mobiles},email:{email},legitId:{legitId}});",**usr)

    def createPhoneContact(self,data):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                data = {'appfbid':data['appFBId']}
                print(data)
                tx.run("Match (u:UserNode{appFBId:{appfbid}}),(p:PhoneContact{appFBId:{appfbid}}) WHERE NOT (u.fullName = p.contactName) MERGE (u)-[:phone_contacts]->(p)",**data)

    def createSameContact(self,data):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                data = {'appFBId':data['appFBId']}
                tx.run("MATCH (n:UserNode{appFBId:{appFBId}})-[:phone_contacts]->(p:PhoneContact) with p match(p2:PhoneContact{phoneNumber:p.phoneNumber})  where not p=p2 merge (p)-[:same_contact]-(p2)",**data)

    def createUserNumber(self,data):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                data = {'appFBId':data['appFBId']}
                tx.run("match (u:UserNode{appFBId:{appFBId}}) create unique (u)-[:user_contact]->(:PhoneContact {contactName:u.fullName,phoneNumber:u.mobile,contactType:'mobile',appFBId:u.appFBId})",**data)


class DocManager(DocManagerBase):
    """DocManager that connects to MyIndexingSystem"""

    # methods will go here
    def __init__(self, url, auto_commit_interval=constants.DEFAULT_COMMIT_INTERVAL,unique_key='_id', chunk_size=constants.DEFAULT_MAX_BULK, **kwargs):
        self.auto_commit_interval = auto_commit_interval
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        self.neo_connect = Neo4jconnector()
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