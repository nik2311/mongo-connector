from mongo_connector.doc_managers.doc_manager_base import DocManagerBase


from mongo_connector import errors, constants
from neo4j.v1 import GraphDatabase
import os
import functools


def getClusterLeader(driver,knowhost,neouser,neopass,protocol='bolt'):
    if not driver:
        driver = GraphDatabase.driver(knowhost, auth=(neouser,neopass))
    with driver.session() as session:
        with session.begin_transaction() as tx:
            d=tx.run("call dbms.cluster.overview()")
            info = d.data()
            leader = list(filter(lambda x : x['role']=="LEADER",info))
            if len(leader) > 0:
                leader = leader[0]
                uri = list(filter(lambda x : re.match('.*'+protocol+'.*',x),leader['addresses']))
                return uri[0] if len(uri) > 0 else None

def neomaster(func):
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            try:
                print("Writing to host : ",self.uri)
                query,params=func(self, *args, **kwargs)
                if query and params:
                    with self.driver.session() as session:
                        with session.begin_transaction() as tx:
                            tx.run(query,**params)
            except Exception as e:
                print(e)
                leader = getClusterLeader(self.driver,self.uri,self.neo4juser,self.neo4jpass)
                self.uri = leader if leader else uri
                self.driver = GraphDatabase.driver(uri, auth=(self.neo4juser,self.neo4jpass))
        return wrap


class Neo4jconnector(object):
    
    def __init__(self,uri="bolt://localhost:7687",auth=("neo4j", "neodev")):
        self.uri = os.environ.get('NEO4J_HOST','bolt://localhost:7687')
        self.neo_user = os.environ.get('NEO4J_USER','neo4j')
        self.neo_pass = os.environ.get('NEO4J_PASS','neo4j!123')
        self.auth = (self.neo_user,self.neo_pass)
        self.driver = GraphDatabase.driver(self.uri, auth=self.auth)


    
    def createUser(self,user,namespace=None):
        if namespace=='appusers':
            usr = dict()
            usr['appFBId'] = user['appFBId']
            usr['fullName'] = user["fullName"]
            usr['mobiles']=user['mobileNumber']
            usr['email']=user.get('email',None)
            usr['legitId']=user.get('legit_id',None)
            usr['objId']=str(user.get('_id',''))
            print("CREATE USER ",usr)
            query = "Create (u:UserNode {appFBId:{appFBId},fullName:{fullName},mobile:{mobiles},email:{email},legitId:{legitId},ObjId:{objId}});"
            self.executeQuery(query,usr)
        else:
            return None,None
    
    
    def updateUser(self,user,namespace=None):
        if namespace=='appusers':
            usr = dict()
            usr['appFBId'] = user['appFBId']
            usr['fullName'] = user["fullName"]
            usr['mobiles']=user['mobileNumber']
            usr['email']=user.get('email',None)
            usr['appFBId'] = user['appFBId']
            query = "Match (u:UserNode {appFBId:{appFBId}}) Set u.fullName={fullName},u.mobile={mobiles},u.email={email};"
            self.executeQuery(query,usr)
        else:
            return None,None

    @neomaster
    def executeQuery(self,query,params):
        return query,params

    def createNewPhoneContact(self,data,namespace=None):
        if namespace=='appuserphonecontacts':
            for phonedata in data.get('contactRow',{}).get('phones',[]):
                phone = dict()
                phone['appFBId'] = data.get('appFBId',None)
                if phone['appFBId'] :
                    phone['contactName'] = data['contactRow']["fullName"]
                    phone['phoneNumber']=phonedata['data']
                    print("CREATE CONTACT ",phone)
                    query = "Create (u:PhoneContact {appFBId:{appFBId},contactName:{contactName},phoneNumber:{phoneNumber}});"
                    self.executeQuery(query,phone)
                    self.createPhoneContactRelation({'appFBId':phone['appFBId']})
                    self.createSameContact({'appFBId':phone['appFBId']})
        

    def createPhoneContactRelation(self,data,namespace=None):
        query = "Match (u:UserNode{appFBId:{appfbid}}),(p:PhoneContact{appFBId:{appfbid}}) WHERE NOT (u.fullName = p.contactName) MERGE (u)-[:phone_contacts]->(p)"
        data = {'appfbid':data['appFBId']}
        self.executeQuery(query,data)

    def createSameContact(self,data,namespace=None):
        query = "MATCH (n:UserNode{appFBId:{appFBId}})-[:phone_contacts]->(p:PhoneContact) with p match(p2:PhoneContact{phoneNumber:p.phoneNumber})  where not p=p2 merge (p)-[:same_contact]-(p2)"
        data = {'appFBId':data['appFBId']}
        self.executeQuery(query,data)

    def createUserNumber(self,data,namespace=None):
        query = "match (u:UserNode{appFBId:{appFBId}}) create unique (u)-[:user_contact]->(:PhoneContact {contactName:u.fullName,phoneNumber:u.mobile,contactType:'mobile',appFBId:u.appFBId})"
        data = {'appFBId':data['appFBId']}
        self.executeQuery(query,data)


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
        # print("INSERT :",doc,namespace)
        self.neo_connect.createUser(doc,namespace=namespace.split('.')[1])
        self.neo_connect.createNewPhoneContact(doc,namespace=namespace.split('.')[1])

    def update(self, document_id, update_spec, namespace, timestamp):
        # print("UPDATE :",update_spec,namespace)
        self.neo_connect.updateUser(update_spec,namespace=namespace.split('.')[1])

    def remove(self, document_id, namespace, timestamp):
        print("DELETE : ",document_id,namespace)

    def search(self, start_ts, end_ts):
        pass
        # print("search from {0} to {1}".format(start_ts,end_ts))

    def handle_command(self, doc, namespace, timestamp):
        db = namespace.split('.', 1)[0]
        pass
        # print(db,namespace)