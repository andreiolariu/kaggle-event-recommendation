from math import isnan

import placemaker

'''
    Area for yahoo placemaker and other 
    location finding functions
'''
YAHOO_APPID = 'dj0yJmk9OG9SNGFTbmphTTZiJmQ9WVdrOVMxVnBhbXh3TldjbWNHbzlNamcyTXpnek1EWXkmcz1jb25zdW1lcnNlY3JldCZ4PTMw'
YAHOO_TYPES = ['Commune', 
        'Municipality', 
        'District', 
        'Ward',
        'POI', 
        'Town', 
        'Suburb', 
        'Postal Code',
        'LocalAdmin',
        'County', 
        'Zip']
p = placemaker.placemaker(YAHOO_APPID)
        
def encode_utf8(s):
  if isinstance(s, unicode):
    return s.encode('utf-8')
  try:
    s.decode('utf-8')
    return str(s)
  except:
    pass
  try:
    return str(s.encode('utf-8'))
  except:
    res = ''.join([unichr(ord(c)) for c in s])
    return str(res.encode('utf-8'))
    
def get_coordinates_from_text(text):
    # use yahoo placemaker to search for locations
    try:
        location = p.find_places(encode_utf8(text))
        time.sleep(0.1)
    except Exception, e:
        print 'Error in placemaker: %s' % e
        time.sleep(5)
        location = []
    if len(location) == 0:
        return None
    location = location[0]
    print 'output %s - %s' % (location.placetype, location.name)
    #if location.placetype in YAHOO_TYPES:
    return [location.centroid.latitude, 
            location.centroid.longitude]
    print 'nothing for ' + text
    return None
    
location_cache = {}

'''
    Putting user info in a dictionary
'''
'''
user_info = {}
user_info_file = pd.read_csv("users.csv")

# Populate dictionary
for user in user_info_file.iterrows():
    user = user[1]
    location = None
    
    # process location
    if isinstance(user['location'], str):
        if user['location'] in location_cache:
            location = location_cache[user['location']]
        else:
            location = get_coordinates_from_text(user['location'])
            if location:
                location_cache[user['location']] = location
    else:
        location = None
    
    
    user_info[user['user_id']] = {
        'birth': user['birthyear'],
        'gender': user['gender'],
        'location': location,
    }
    
# Alternate code for writing to mongo
data = json.loads(open('data', 'r').read())
for uid, user in data.iteritems():
    user['id'] = int(uid)
    user_info.insert([user])

'''

   
'''
    Create attendance dictionaries
    Provided in both directions
'''
'''
def update_attendance(uid, eid, att_type):
    attendance.update(
        {'uid': uid, 'eid': eid},
        {'$set': {'uid': uid, 'eid': eid, att_type: True}}, 
        upsert=True)

chunks = pd.read_csv("events.csv", iterator=True, chunksize=1000)

count = 0
for chunk in chunks:
    print count
    count += 1000
    
    for e in chunk.iterrows():
        e = e[1]
        eid = e['event_id']
        uid = e['user_id']
        
        update_attendance(uid, eid, 'yes')
        update_attendance(uid, eid, 'interested')
'''    
    
'''
    Populate events dictionary
'''
'''
chunks = pd.read_csv("events.csv", iterator=True, chunksize=1000)

count = 0
for chunk in chunks:
    print count
    count += 1000
    #if count < 1564000:
    #    continue
    for e in chunk.iterrows():
        e = e[1]
        eid = e['event_id']
        location = None
        if e['lat'] and e['lng'] and not isnan(e['lat']) and not isnan(e['lng']):
            location = [e['lat'], e['lng']]
        elif e['city'] or e['state'] or e['country']:
            loc_string = '%s, %s, %s' % (e['city'], e['state'], e['country'])
            if loc_string in location_cache:
                location = location_cache[loc_string]
            else:
                location = get_coordinates_from_text(loc_string)
                if location:
                    location_cache[loc_string] = location
                    
        words = list(e[9:110])
        event = {
            'id': eid,
            'location': location,
            'words': words
        }
        event_info.insert([event])
        
chunks = pd.read_csv("events.csv", iterator=True, chunksize=1000)

count = 0
for chunk in chunks:
    print count
    count += 1000
    for e in chunk.iterrows():
        e = e[1]
        eid = e['event_id']
        uid = e['user_id']
        # add creator to event attendance collection
        update_attendance(uid, eid, 'yes')
        update_attendance(uid, eid, 'interested')
'''       
        
'''
    Insert train data into attendance collection
'''
'''
train = pd.read_csv( "train.csv", converters={"timestamp": parse})

for pair in train.iterrows():
    pair = pair[1]
    uid = pair['user']
    eid = pair['event']
    for attr in ['invited', 'interested', 'not_interested']:
        if pair[attr]:
            update_attendance(uid, eid, attr)
'''

''' 
    Process event_attendees file to update attendance collection
'''
'''
event_attendees = pd.read_csv("event_attendees.csv")

for event in event_attendees.iterrows():
    event = event[1]
    eid = event['event']
    for attr in ['yes', 'maybe', 'invited', 'no']:
        users = event[attr]
        if isinstance(users, float):
            continue
        users = [int(u) for u in users.split()]
        for uid in users:
            update_attendance(uid, eid, attr)
'''

''' 
    Process friends file
'''
'''
friends_file = pd.read_csv('user_friends.csv')
friends_dict = {}

# create friends data in memory
for record in friends_file.iterrows():
    record = record[1]
    uid1 = record['user']
    if uid1 not in friends_dict:
        friends_dict[uid1] = []
    friends = record['friends']
    if isinstance(friends, float):
        continue
    friends = [int(u) for u in record['friends'].split()]
    for uid2 in friends:
        friends_dict[uid1].append(uid2)
        
# copy data to mongo
for uid, friends in friends_dict.iteritems():
    record = {
        'uid': uid,
        'friends': list(friends)
    }
    friends_db.insert([record])
'''

''' 
    Fill out missing location
'''
user_sure = {}
event_sure = {}
for user in user_info.find():
    if user['location']:
        user_sure[user['id']] = user['location']
        
for event in event_info.find():
    if event['location']:
        event_sure[event['id']] = event['location']
        
count = 0
for event in event_info.find():
    count += 1
    if count % 10000 == 0:
        print count
    location = event['location']
    if location:
        if len(location) == 2 and isinstance(location[0], float) and \
            (isnan(location[0]) or isnan(location[1])):
                location = None
        else:
            if isinstance(location[0], list):
                location = [tuple(l) for l in location]
            else:
                location = tuple(location)
                
    event_info.update({'id': event['id']},
        {'$set': {'location': location}})
   
count = 0
updated = 0
for event in event_info.find():
    count += 1
    if count % 1000 == 0:
        print count
        
    if event['id'] in event_locations:
        continue
    
    attend = attendance.find({'eid': event['id']})
    votes = {}
    
    for a in attend:
        if 'yes' not in a:
            continue
        if a['uid'] in user_sure:
            location = user_sure[a['uid']]
            if isinstance(location[0], float) or isinstance(location[0], int):
                location = [location]
            for l in location:
                l = tuple(l)
                votes[l] = votes.get(l, 0) + 1
    
    if not votes: 
        continue
        
    votes = votes.items()
    votes.sort(key=lambda x: -x[1])
    votes = votes[:5]
    if len(votes) > 1 and votes[0][1] > 3 * votes[1][1]:
        votes = [votes[1]]
        
    votes = [list(v[0]) for v in votes]
    
    if len(votes) == 1:
        votes = votes[0]
    
    print votes
    updated += 1
    event_info.update({'id': event['id']},
        {'$set': {'location': votes}})
    
 
count = 0
updated = 0
for user in user_info.find():
    count += 1
    if count % 1000 == 0:
        print count
        
    if user['id'] in user_sure:
        continue
    
    attend = attendance.find({'uid': user['id']})
    votes = {}
    
    for a in attend:
        if a['eid'] in event_sure:
            location = event_sure[a['eid']]
            if isinstance(location[0], float) or isinstance(location[0], int):
                location = [location]
            for l in location:
                votes[tuple(l)] = votes.get(tuple(l), 0) + 1
    
    if not votes: 
        continue
    
    votes = votes.items()
    votes.sort(key=lambda x: -x[1])
    votes = votes[:5]
    if len(votes) > 1 and votes[0][1] > 3 * votes[1][1]:
        votes = [votes[1]]
        
    votes = [list(v[0]) for v in votes]
    user_sure[user['id']] = votes
    
    updated += 1
    user_info.update({'id': user['id']},
        {'$set': {'location': votes}})
    
for user in user_info.find():
    if not user['location']:
        continue
    user_info.update({'id': user['id']},
        {'$set': {'location': tuple(user['location'])}})
    

user_dict = list(user_info.find())
user_dict = {u['id']:u for u in user_dict}
# create event profiles based on users

event_ids = set([])
for u in user_info.find():
    if u['age']:
        attends = attendance.find({'uid': u['id']})
        
    

count = event_info.count()
for e in event_info.find():
    attends = attendance.find({'eid': e['id']})
    count -= 1
    if count % 10000 == 0:
        print count
    ages = []
    sexes = {'male':0, 'female':0}
    for at in attends:
        if 'yes' not in attends:
            continue
        uid = at['uid']
        if uid in user_dict:
            a = user_dict[uid]['age']
            if a:
                ages.append(a)
    
    if ages:
        age = np.mean(ages)
        print age
        break
        event_info.update({'id': e['id']},
            {'$set': {'age': age}})
        
        
        
user_info_file = pd.read_csv("users.csv")

updated = 0
for user in user_info_file.iterrows():
    user = user[1]
    location = None
    
    if user['user_id'] in user_sure:
        continue
    
    # process location
    if isinstance(user['location'], str):
        location = get_coordinates_from_text(user['location'])
    else:
        location = None
    
    if location:
        updated += 1
        user_sure[user['user_id']] = [location]
        user_info.update({'id': user['user_id']},
                {'$set': {'location': [location]}})
        
        
count = 0
updated = 0
friends_info = {}
for user in user_info.find():
    count += 1
    if count % 1000 == 0:
        print count
        
    if user['id'] in user_sure:
        continue
    
    friends = friends_db.find_one({'uid': user['id']})
    friends = friends['friends']
    friends = list(user_info.find({'id':{'$in': friends}}))
    friends_info[user['id']] = friends

updated = 0
for uid, friends in friends_info.iteritems():
    s = 0
    nr = 0
    
    for f in friends:
        if f['age']:
            s += f['age']
            nr += 1
    if nr:
        guess = s * 1.0 / nr
        user_info.update({'id': uid},
            {'$set': {'age': guess}})
        updated += 1
   
    

count = 0
event_locations = {}
for e in event_info.find():
    count += 1
    if count % 10000 == 0:
        print count
    if e['location']:
        event_locations[e['id']] = e['location']
        
count = 0
updated = 0
for event in event_info.find():
    count += 1
    if count % 10000 == 0:
        print count
    
    location = event['location']
    if location:
        continue
    loc_string = event_locations.get(event['id'])
    if not loc_string or loc_string == 'nan, nan, nan':
        continue
    
    if loc_string in location_cache:
        location = location_cache[loc_string]
    else:
        location = get_coordinates_from_text(loc_string)
        if location:
            location_cache[loc_string] = location
          
    if location:
        updated += 1
        event_info.update({'id': event['id']},
            {'$set': {'location': location}})
    
for u in user_info.find():
    a = u['birth']
    try:
        a = int(a)
        if a < 1940:
            a = None
        else:
            a = 2013 - a
    except:
        a = None
    ages[a] = ages.get(a, 0) + 1
    user_info.update({'id': u['id']},
            {'$set': {'age': a}})
    
user_sure = {}
for u in user_info.find():
    a = u['age']
    if a:
        user_sure[u['id']] = a
        
        
# set user timezone from file
user_info_file = pd.read_csv("users.csv")
user_tz = {}
updated = 0
for user in user_info_file.iterrows():
    user = user[1]
    tz = user['timezone']
    if math.isnan(tz):
        tz = None
    user_tz[user['user_id']] = tz
    
for uid, tz in user_tz.iteritems():
    user_info.update({'id': uid},
            {'$set': {'timezone': tz}})
        
# propagate timezone
etz = {}
for a in attendance.find():
    eid = a['eid']
    uid = a['uid']
    if uid not in user_tz:
        continue
    if 'yes' not in a and 'maybe' not in a:
        continue
    if eid not in etz:
        etz[eid] = {}
    tz = user_tz[a['uid']]
    etz[eid][tz] = etz[eid].get(tz, 0) + 1 
    
etzf = {}
for eid, tz_dict in etz.iteritems():
    tzl = tz_dict.items()
    tzl.sort(key=lambda x: -x[1])
    etzf[eid] = tzl[0]
    
    
count = len(etzf)
for eid, tz in etzf.iteritems():
    count -= 1
    if count % 10000 == 0:
        print count
    event_info.update({'id': eid},
        {'$set': {'timezone': tz}})
    
    
    

chunks = pd.read_csv("events.csv", iterator=True, chunksize=10000)

count = 0
for chunk in chunks:
    print count
    count += 10000
    for e in chunk.iterrows():
        e = e[1]
        t = e['start_time']
        t = parse(t)
        t = time.mktime(t.timetuple())
        event_info.update({'id': e['event_id']},
            {'$set': {'start': t}})
