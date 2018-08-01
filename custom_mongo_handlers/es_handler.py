import os
import json
from decimal import Decimal
from elasticsearch import Elasticsearch

class CustomElasticFeeder(object):
        

    def read_config(self):
        conf_path=os.environ.get('ELASTIC_CONF',None)
        if conf_path:
            with open(conf_path) as f:
                data = json.load(f)
            return data

    def __init__(self):
        self.config = self.read_config()
        self.es = Elasticsearch()

    def upsert(self, doc, namespace, timestamp):
        stat_date = doc['stat_date']
        created = doc['created']
        data=[{'stat_date':stat_date,'created':created,'Campaign (c)':x.get('Campaign (c)',''),'ARPU':float(x.get('ARPU','0.0')),'Installs':int(x.get('Installs',0))} for x  in doc['flyer_data']]
        for d in data:
            res = self.es.index(index="appflyermonitorstats", doc_type='appflyermonitorstats', id=str(doc['_id']), body=d)


    def update(self, document_id, update_spec, namespace, timestamp):
        print("UPDATE custom:",update_spec,namespace)

    def remove(self, document_id, namespace, timestamp):
        print("DELETE : ",document_id,namespace)