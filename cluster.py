from sklearn.cluster import MiniBatchKMeans

# this now clusters in 100 or 400 clusters
# I also used cluster numbers of 8, 20 and 40
cluster_names = [(8, 'cl0'), (20, 'cl1'), (40, 'cl2')]
ckeys = [c[1] for c in cluster_names]
kms = {}
for nclust, key in cluster_names:
    km = MiniBatchKMeans(n_clusters=nclust)
    X = None
    count = 0
    for e in event_info.find():
        if X is None:
            X = np.array(e['words'])
        else:
            X = np.vstack((X, e['words']))
        count += 1
        
        if count % 10000 == 0:
            km.partial_fit(X)
            X = None
            print count
    
    kms[key] = km 
    
event_clusters = {}
for e in event_info.find():
    clusters = {key: int(km.predict(e['words'])[0]) for key, km in kms.iteritems()}
    event_info.update({'id': e['id']},
        {'$set': clusters})
    
    
# load database data, for fast access
event_clusters = {}
for e in event_info.find():
    event_clusters[e['id']] = {k: e[k] for k in ckeys}

user_attend_dict = {}
count = attendance.count()
for a in attendance.find():
    count -= 1
    if count % 100000 == 0:
        print count
    if 'invited' in a:
        uid = a['uid']
        eid = a['eid']
        if uid not in user_attend_dict:
            user_attend_dict[uid] = []
        user_attend_dict[uid].append(eid)


# get the clusters for events an user attends:
count = 0
f2 = 0
updated = 0
for u in user_info.find():
    count += 1
    if count % 1000 == 0:
        print count
    uid = int(u['id'])
    #attend_list_ids = user_attend_dict.get(uid, [])
    attend_list_ids = list(attendance.find({'uid': uid}))
    attend_list_ids = [a['eid'] for a in attend_list_ids if 'invited' in a]
    e_list = list(event_info.find({'id':{'$in': attend_list_ids}}))
    
    if not e_list:
        # no events :(
        f2 += 1
        continue
    
    taste = {}
    taste.update({
        'cl0': [0] * 8,
        'cl1': [0] * 20,
        'cl2': [0] * 40,
    })
    
    for e in e_list:
        c = event_clusters[e['id']]
        for cl in ckeys:
            taste[cl][c[cl]] += 1
        
    # normalize taste
    for cl in ckeys:
        s = sum(taste[cl]) * 1.0
        taste[cl] = [i / s for i in taste[cl]]
        
    updated += 1
    user_info.update({'id': uid},
        {'$set': {'user_invited': taste}})
          
          
          
# get the clusters for events a user's friends attend
count = 0
updated = 0
f1 = 0
f2 = 0
for u in user_info.find():
    count += 1
    if count % 1000 == 0:
        print count
    uid = int(u['id'])
    friends = friends_db.find_one({'uid': uid})
    if not friends:
        f1 += 1
        continue
        
    friends = friends['friends']
    events = set([])
    for f in friends:
        events.update(user_attend_dict.get(f, []))
        
    taste = {}
    taste.update({
        'cl0': [0] * 8,
        'cl1': [0] * 20,
        'cl2': [0] * 40,
    })
    
    sw = 0
    for eid in events:
        if eid not in event_clusters:
            continue
        c = event_clusters[eid]
        for cl in ckeys:
            taste[cl][c[cl]] += 1
        sw = 1
        
    if sw == 0:
        # no friends or no events :(
        f2 += 1
        continue
    
    # normalize taste
    for cl in ckeys:
        s = sum(taste[cl]) * 1.0
        taste[cl] = [i / s for i in taste[cl]]
    
    # update
    updated += 1
    user_info.update({'id': uid},
        {'$set': {'friends_invited': taste}})
        
