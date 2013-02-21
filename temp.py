solutions_dict = get_test_solutions()
count = 0
for uid, eidr in solutions_dict.iteritems():
    eidr = eidr[0]
    if not (eidr == results2[uid][0] and eidr != results[uid][0]):
        continue
    count += 1
    l = results[uid]
    user = user_info.find_one({'id': uid})
    print '-----------'
    print '-----------'
    print user['location']
    print user['newloc']
    print '-----------'
    print ''
    for i, eid in enumerate(l):
        if eid == eidr:
            print '--- %s' % eid
        else:
            print '%s - %s' % (i, eid)
        event = event_info.find_one({'id': eid})
        print event.get('location', '-')
        print get_location_distance(event['location'], user['location'])
        print event.get('newloc', [])
        print process_locations(event.get('newloc', []), user['newloc'])
        print ''
        
        
        


components = {}
for f in friends_db.find():
    components[f['uid']] = f['uid']
    for uid in f['friends']:
        components[uid] = uid

def query(id1):
    parent = components[id1]
    while components[parent] != parent:
        tmp = parent
        parent = components[parent]
        components[tmp] = components[parent]
    components[id1] = parent
    return parent

def unite(id1, id2):
    id1 = query(id1)
    id2 = query(id2)
    if id1 != id2:
        components[id1] = id2
        
count = friends_db.count()
for f in friends_db.find():
    count -= 1
    print count
    id1 = f['uid']
    for id2 in f['friends']:
        unite(id1, id2)
        
# surface all
count = len(components)
for id1 in components.iterkeys():
    count -= 1
    if count % 100000 == 0:
        print count
    query(id1)
    
f = open('uids_comps.json', 'w')
for id1, parent in components.iteritems():
    f.write('%s %s\n' % (id1, parent))
f.close()

# exit and enter - to free ram
from graph_tool.all import *

f = open('uids_comps.json', 'r')
count = 0
while True:
    count += 1
    if count % 10000 == 0:
        print count
    line = f.readline()
    if not line:
        break
    line = line.split(' ')
    id1, id2 = [int(i) for i in line]
    g.add_edge(id1, id2)
