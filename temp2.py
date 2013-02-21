import time

no_attend = {}
not_int_attend = {}
attend = {}
invited = {}
   
for  att in attendance.find():
    uid = att['uid']
    '''
    if 'no' in att or 'not_interested' in att:
        if 'no' in att:
            if uid not in no_attend:
                no_attend[uid] = []
            no_attend[uid].append(att['eid'])
        if 'not_interested' in att:
            if uid not in not_int_attend:
                not_int_attend[uid] = []
            not_int_attend[uid].append(att['eid'])
    
    if 'yes' in att or 'maybe' in att or 'interested' in att:
        if uid not in attend:
            attend[uid] = []
        attend[uid].append(att['eid'])
    '''
    if 'invited' in att:
        if uid not in invited:
            invited[uid] = []
        invited[uid].append(att['eid'])
    


count = 0
updated = 0
for user in user_info.find():
    count += 1
    if count % 100 == 0:
        print '%s - %s' % (count, time.time())
    uid = user['id']
    friends = friends_db.find_one({'uid': uid})
    fids = [uid]
    event_ids = set([])
    if friends:
        fids += friends.get('friends', [])
    for fid in fids:
        event_ids.update(attend.get(fid, []))
    events = list(event_info.find({'id': {'$in': list(event_ids)}}))
    if not events:
        if 'prototype' in user:
            user_info.update({'id': uid},
                {'$set': {'prototype': None}}) 
        continue
    updated += 1
    s = np.array([0.0] * 101)
    for event in events:
        s += event['words']
    s /= np.sum(s)
    s = list(s)
    user_info.update({'id': uid},
        {'$set': {'prototype': s}}) 
    
    
event_simms = []
count = 0
updated = 0
for user in user_info.find():
    count += 1
    if count % 100 == 0:
        print '%s - %s' % (count, time.time())
    uid = user['id']
    friends = friends_db.find_one({'uid': uid})
    fids = []
    event_ids = set([])
    if friends:
        fids += friends.get('friends', [])
    for fid in fids:
        event_ids.update(invited.get(fid, []))
    events = list(event_info.find({'id': {'$in': list(event_ids)}}))
    if not events:
        continue
    updated += 1
    s = np.array([0.0] * 101)
    for event in events:
        s += event['words']
    s /= np.sum(s)
    s = list(s)
    user_info.update({'id': uid},
        {'$set': {'prototype_invite': s}}) 
    if 'prototype' in user:
        event_simms.append(get_event_distance(user['prototype'], s))
    
    
# ---------------------
# process locations based on attendance and friends... again
user_sure = {}
event_sure = {}
for user in user_info.find():
    if 'newloc2' in user and user['newloc2']:
        user_sure[user['id']] = user['newloc2']
        
'''
for event in event_info.find():
    if 'newloc2' in event and event['newloc2']:
        event_sure[event['id']] = event['newloc2']
'''
  
attended = {}
for att in attendance.find():
    if not ('yes' in att or 'maybe' in att):
        continue
    eid = att['eid']
    uid = att['uid']
    if eid not in attended:
        attended[eid] = []
    attended[eid].append(uid)
    
count = 0
updated = 0
for event in event_info.find():
    count += 1
    if count % 1000 == 0:
        print count
        
    if event.get('newloc2', None):
        continue
    
    attend = attended.get(event['id'], [])
    votes = {}
    for uid in attend:
        if uid in user_sure:
            locations = user_sure[uid]
            for l in locations:
                key = (l.get('city'), l.get('state'), l.get('country'))
                votes[key] = votes.get(key, 0) + 1
    
    if not votes: 
        continue
        
    votes = votes.items()
    votes.sort(key=lambda x: -x[1])
    i = 1
    while len(votes) > i and votes[i-1][1] < votes[i][1] * 4:
        i += 1
    votes = votes[:i]
        
    locations = []
    for v in votes:
        l = {}
        v = v[0]
        l['country'] = v[2]
        if v[1]:
            l['state'] = v[1]
        if v[0]:
            l['city'] = v[0]
        locations.append(l)
    
    updated += 1
    continue
    event_info.update({'id': event['id']},
        {'$set': {'newloc2': locations}})
    
    
    
    

attended = {}
for att in attendance.find():
    if not ('yes' in att or 'maybe' in att):
        continue
    eid = att['eid']
    uid = att['uid']
    if uid not in attended:
        attended[uid] = []
    attended[uid].append(eid)
    
count = 0
updated = 0
for user in user_info.find():
    count += 1
    if count % 1000 == 0:
        print count
    
    if user.get('newloc2', None):
        continue
    
    attend = attended.get(user['id'], [])
    events = []
    for event in event_info.find({'id': {'$in': attend}}):
        if 'newloc2' in event and event['newloc2']:
            events.append(event)
    votes = {}
    for event in events:
        locations = event['newloc2']
        for l in locations:
            key = (l.get('city'), l.get('state'), l.get('country'))
            votes[key] = votes.get(key, 0) + 1
    friends = friends_db.find_one({'uid': user['id']})
    if friends:
        for fid in friends['friends']:
            if fid in user_sure:
                locations = user_sure[fid]
                for l in locations:
                    key = (l.get('city'), l.get('state'), l.get('country'))
                    votes[key] = votes.get(key, 0) + 1
        
    if not votes: 
        continue
        
    votes = votes.items()
    votes.sort(key=lambda x: -x[1])
    i = 1
    while len(votes) > i and votes[i-1][1] < votes[i][1] * 4:
        i += 1
    votes = votes[:i]
        
    locations = []
    for v in votes:
        v = v[0]
        l = {}
        l['country'] = v[2]
        if v[1]:
            l['state'] = v[1]
        if v[0]:
            l['city'] = v[0]
        locations.append(l)
    
    
    updated += 1
    user_sure[user['id']] = locations
    user_info.update({'id': user['id']},
        {'$set': {'newloc2': locations}})
    
    
    
# reprocess user ages
uages = {}
count = 0
for u in user_info.find():
    count += 1
    if count % 2000 == 0:
        print count
        
    a = u['birth']
    try:
        a = int(a)
        a = 2012 - a
    except:
        a = None
    if not a:
        continue
    group = None
    if 0 < a <= 11:# it appears there's nobody in this group
        group = 0
    elif 12 <= a <= 16:
        group = 1
    elif 17 <= a <= 20:
        group = 2
    elif 21 <= a <= 25:
        group = 3
    elif 26 <= a <= 33:
        group = 4
    elif 34 <= a <= 62:
        group = 5
    if group is None:
        continue
        
    uages[u['id']] = group
    user_info.update({'id': u['id']},
            {'$set': {'age_group': group}})
    
count = 0
updated = 0
for u in user_info.find():
    count += 1
    if count % 2000 == 0:
        print count
    
    if u['id'] in uages:
        continue
    
    friends = friends_db.find_one({'uid': u['id']})
    if not friends:
        continue
    friends = friends['friends']
    friends = [f for f in friends if f in uages]
    if not friends:
        continue
    votes = {}
    for fid in friends:
        votes[uages[fid]] = votes.get(uages[fid], 0) + 1
    votes = votes.items()
    votes.sort(key=lambda x: -x[1])
    if len(votes) > 1 and votes[0][1] == votes[1][1]:
        continue
        
    group = votes[0][0]
    updated += 1
    user_info.update({'id': u['id']},
            {'$set': {'age_group': group}})

## updated for events
uages = {}
for u in user_info.find():
    if 'age_group' not in u:
        continue
    uages[u['id']] = u['age_group']
    
attended = {}
count = 0
for att in attendance.find():
    count += 1
    if count % 100000 == 0:
        print count
    if not ('yes' in att or 'maybe' in att):
        continue
    uid = att['uid']
    if uid not in uages:
        continue
    eid = att['eid']
    if eid not in attended:
        attended[eid] = []
    attended[eid].append(uages[uid])

updated = 0
count = 0
for e in event_info.find():
    count += 1
    if count % 50000 == 0:
        print count
    eid = e['id']
    if eid not in attended:
        continue
    distr = [0,1,1,1,1,1]
    for g in attended[eid]:
        distr[g] += 1
    if sum(distr) < 9:
        continue
    updated += 1
    
