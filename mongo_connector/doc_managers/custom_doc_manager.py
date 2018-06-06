from mongo_connector.doc_managers.doc_manager_base import DocManagerBase


from mongo_connector import errors, constants
from neo4j.v1 import GraphDatabase
import os



class Neo4jconnector(object):
    
    def __init__(self,uri="bolt://localhost:7687",auth=("neo4j", "neodev")):
        uri = os.environ.get('NEO4J_HOST','bolt://localhost:7687')
        neo_user = os.environ.get('NEO4J_USER','neo4j')
        neo_pass = os.environ.get('NEO4J_PASS','neo4j!123')
        auth = (neo_user,neo_pass)
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def createUser(self,user,namespace=None):
        if namespace=='appusers':
            usr = dict()
            usr['appFBId'] = user['appFBId']
            usr['fullName'] = user["fullName"]
            usr['mobiles']=user['mobileNumber']
            usr['email']=user.get('email',None)
            usr['legitId']=user.get('legit_id',None)
            with self.driver.session() as session:
                with session.begin_transaction() as tx:
                    tx.run("Create (u:UserNode {appFBId:{appFBId},fullName:{fullName},mobile:{mobiles},email:{email},legitId:{legitId}});",**usr)

    def updateUser(self,user,namespace=None):
        if namespace=='appusers':
            usr = dict()
            usr['appFBId'] = user['appFBId']
            usr['fullName'] = user["fullName"]
            usr['mobiles']=user['mobileNumber']
            usr['email']=user.get('email',None)
            usr['appFBId'] = user['appFBId']
            with self.driver.session() as session:
                with session.begin_transaction() as tx:
                    tx.run("Match (u:UserNode {appFBId:{appFBId}}) Set u.fullName={fullName},u.mobile={mobiles},u.email={email};",**usr)

    
    def createNewPhoneContact(self,data,namespace=None):
        if namespace=='appuserphonecontacts':
            for phonedata in data.get('contactRow',{}).get('phones',[]):
                phone = dict()
                phone['appFBId'] = data.get('appFBId',None)
                if phone['appFBId'] :
                    phone['contactName'] = data['contactRow']["fullName"]
                    phone['phoneNumber']=phonedata['data']
                    print(phone)
                    with self.driver.session() as session:
                        with session.begin_transaction() as tx:
                            tx.run("Create (u:PhoneContact {appFBId:{appFBId},contactName:{contactName},phoneNumber:{phoneNumber}});",**phone)
                            self.createPhoneContactRelation({'appFBId':phone['appFBId']})
                            self.createSameContact({'appFBId':phone['appFBId']})
        

    def createPhoneContactRelation(self,data,namespace=None):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                data = {'appfbid':data['appFBId']}
                print(data)
                tx.run("Match (u:UserNode{appFBId:{appfbid}}),(p:PhoneContact{appFBId:{appfbid}}) WHERE NOT (u.fullName = p.contactName) MERGE (u)-[:phone_contacts]->(p)",**data)

    def createSameContact(self,data,namespace=None):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                data = {'appFBId':data['appFBId']}
                tx.run("MATCH (n:UserNode{appFBId:{appFBId}})-[:phone_contacts]->(p:PhoneContact) with p match(p2:PhoneContact{phoneNumber:p.phoneNumber})  where not p=p2 merge (p)-[:same_contact]-(p2)",**data)

    def createUserNumber(self,data,namespace=None):
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
        print("INSERT :",doc,namespace)
        self.neo_connect.createUser(doc,namespace=namespace.split('.')[1])
        self.neo_connect.createNewPhoneContact(doc,namespace=namespace.split('.')[1])

    def update(self, document_id, update_spec, namespace, timestamp):
        print("UPDATE :",update_spec,namespace)
        self.neo_connect.updateUser(update_spec,namespace=namespace.split('.')[1])

    def remove(self, document_id, namespace, timestamp):
        print("DELETE : ",document_id,namespace)

    def search(self, start_ts, end_ts):
        print("search from {0} to {1}".format(start_ts,end_ts))

    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        print(db,namespace)