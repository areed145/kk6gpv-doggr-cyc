import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import date, datetime, timedelta, time
import json
import numpy as np
from bson import json_util
import random
import os

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        else:
            return super(NpEncoder, self).default(obj)

client = MongoClient(os.environ['MONGODB_CLIENT'])
db = client.petroleum

def convert_entry(api):
    docs = db.doggr.find({'api':api})
    for x in docs:
        doc = dict(x)

    doc.pop('_id')

    try:
        df_prodinj = pd.DataFrame(doc['prodinj'])
        if 'date' not in df_prodinj.columns:
            df_prodinj = df_prodinj.T
        doc['prodinj'] = df_prodinj.to_dict()
    except:
        pass

    df_prodinj = pd.DataFrame(doc['prodinj'])

    try:
        cons = doc['crm']['cons']
        df_cons = pd.DataFrame(cons)
        doc['crm'] = {}
        doc['crm']['cons'] = df_cons.to_dict()
    except:
        pass

    try:
        doc.pop('cyclic_jobs')
    except:
        pass

    try:
        cyclic_jobs = []
        for job in range(df_prodinj['cyclic_ct'].max()):
            if job > 0:
                df_prev = df_prodinj[df_prodinj['cyclic_ct'] == job-1]
                df_prev = df_prev.sort_values(by='date')
                df_prev.reset_index(drop=True, inplace=True)
                df_prev.index = df_prev.index.astype(int) - min(len(df_prev),5)
                df = df_prodinj[df_prodinj['cyclic_ct'] == job]
                df = df.sort_values(by='date')
                df.reset_index(drop=True, inplace=True)
                cyclic_job = {}
                cyclic_job['number'] = job
                cyclic_job['start'] = df['date'].min()
                cyclic_job['end'] = df[df['cyclic'] > 0]['date'].max()
                cyclic_job['total'] = df['cyclic'].sum()
                df_prod = pd.DataFrame()
                df_prod = df_prod.append(df_prev[['oil', 'water']].iloc[:6])
                df_prod = df_prod.append(df[['oil','water']].iloc[:6])
                cyclic_job['prod'] = df_prod.sort_index().to_dict()
                cyclic_jobs.append(cyclic_job)
        df_cyc = pd.DataFrame(cyclic_jobs)
        doc['cyclic_jobs'] = df_cyc.to_dict()
    except:
        df_cyc = None

    entry = json_util.loads(json.dumps(doc, cls=NpEncoder))
    db.doggr.replace_one({'api': api}, entry, upsert=False)

if __name__ == '__main__':
    df_prodinj = pd.DataFrame(list(db.doggr.find({'prodinj': {'$exists': True}},{'api':1})))
    apis = list(set(list(df_prodinj['api'])))
    random.shuffle(apis)
    for api in apis:
        try:
            convert_entry(api)
            print(api,' succeeded')
        except:
            print(api,' failed')
