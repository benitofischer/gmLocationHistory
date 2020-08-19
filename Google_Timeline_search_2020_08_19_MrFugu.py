# !pip install --user pandas==1.0.3
import pandas as pd
import json
from datetime import datetime
from collections import defaultdict
#from pandas.io.json import json_normalize
import os
def os_dir_search(file):
    u=[]
    for p,n,f in os.walk(os.getcwd()):
        
        for a in f:
            a = str(a)
            if a.endswith(file): # can be (.csv) or a file like I did and search 
                print(a)
                print(p)
                t=pd.read_csv(p+'/'+file)
#                               names=['row_id','credit_card',
#                                                 'email','first_name','last_name','primary_phone'],header=0)
            
    return t

os_dir_search('google_addr_file.json')


## Find a file outside your directory:

#def os_any_dir_search(file):
#    u=[]
#    for p,n,f in os.walk(os.getcwd()):
#        
#        for a in f:
#           a = str(a)
#            if a.endswith(file): # can be (.csv) or a file like I did and search 
##                 print(a)
#                print(p)
#                t=pd.read_csv(p+'/'+file)
#                u.append(p+'/'+a)
#    return t,u

#os_any_dir_search('google_addr_file.json')

# with open('google_addr_file.json', 'w') as f_out:
#     json.dump(data , f_out)

googs_=pd.read_json('google_addr_file.json')


# df_=pd.DataFrame(googs_['timelineObjects'])

ext_df_=pd.json_normalize(json.loads(googs_['timelineObjects'].to_json(orient="records")))

ext_df_
desired_vars=['activitySegment.duration.startTimestampMs',
       'activitySegment.duration.endTimestampMs', 
       'activitySegment.distance',
       'activitySegment.activities',
       'placeVisit.location.address',
       'placeVisit.duration.startTimestampMs',
       'placeVisit.duration.endTimestampMs',
       'activitySegment.endLocation.address',
       'activitySegment.endLocation.locationConfidence',
        'placeVisit.location.name',
        'activitySegment.endLocation.name']

# Parse Columns of Interest:
df_start=ext_df_.loc[:,desired_vars]
df_start.dtypes

df_start_=pd.json_normalize(json.loads(df_start.explode('activitySegment.activities').to_json(orient="records")))

ste=[]
for i in df_start:
    if i.find('Timestamp')>0:
        ste.append(i)
#     else:
#         ste.append(0)
len(ste)

df_fill=df_start.loc[:,ste].fillna(0)

df_fill=df_fill.apply(pd.to_numeric)

from collections import defaultdict

tmst=defaultdict(list)

for i in df_fill.to_dict().items():
    for j in i[1].values():
        if j == 0:
            tmst[i[0]].append('Nada')
        else:
            tmst[i[0]].append(datetime.fromtimestamp(j/1000)) # divide by 10^-3 sec

df_start[ste]=pd.DataFrame(tmst)

df_start

df_start[['activitySegment.endLocation.locationConfidence',
'activitySegment.distance']]=df_start.loc[:,['activitySegment.endLocation.locationConfidence',
'activitySegment.distance']].fillna(0)

df_start

df_start['activitySegment.activities']=df_start['activitySegment.activities'].fillna('Nope')

new_df=pd.json_normalize(json.loads(df_start.explode('activitySegment.activities').to_json(orient="records")))

# len(set(new_df['activitySegment.activities.activityType']) )    

tt=[]

for i in df_start['activitySegment.activities'].values.tolist():
    print(type(i))
# creating a representation of original data to remove rows of NaN, to parse easily
trick_=[{'activityType': 'Nada', 'probability': 'Nada'}]*len(df_start['activitySegment.activities'][0])

g=[]
for i in df_start['activitySegment.activities'].values.tolist():
    if type(i)==str: 
        g.append(trick_)
    else:
        g.append(i)


trick_orTreat=pd.DataFrame(g)


wow=pd.json_normalize(json.loads(trick_orTreat.to_json(orient="records")))

'''
Find any string with this word, >0 since we are dealing with location of this word
if i doesn't appear you get -1

'''


act=[]
prob=[]
for i in wow:
    if i.find('activit')>0: 
        act.append(i)
    else:
        prob.append(i)

Prob_=[]       

for i in wow[prob].values:
    Prob_.append(i)


Act_=[]
for j in wow[act].values:
    Act_.append(j)


ohmy=defaultdict(list)


u=[]
for i in Prob_:
    u.extend(i)


v=[]
for i in [Act_[0]]*6: # copying the keys so we have enough to do dict()
    v.extend(i)
# len(v)

for i in list(zip(v,u)):
    ohmy[i[0]].append(i[1])

activities_df=pd.DataFrame(ohmy)
activities_df

col_names=[]
for i in df_start.columns:
    col_names.append(i.split('.',1))
col_names


fin_col_names=[sublist[-1] for sublist in col_names]

df_start.columns=fin_col_names
df_start

# Final DF: 
pd.concat([df_start,activities_df],axis=1)
