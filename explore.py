XT = np.array(X)
YT = np.array(Y1)
XX = []
for t in test_data.itervalues():
    XX.extend(t['X'])
XX = np.array(XX)



requests = {
    'XT': XT,
    'XX': XX,
    'XT0': XT[YT==0],
    'XT1': XT[YT==1],
}
results = {}

for name, matrix in requests.iteritems():
    results[name] = []
    for i in range(len(matrix[0])):
        filtered = np.array(filter(lambda x: x is not None, matrix[:,i]))
        mean = np.mean(filtered)
        std = np.std(filtered)
        nrnull = len(matrix[:,i]) - len(filtered)
        results[name].append({
            'mean': mean,
            'std': std,
            'nrnull': nrnull
        })
        
for i in range(len(XT[0])):
    print '---- variabila ' + str(i)
    for key in ['XT', 'XT1', 'XT0', 'XX']:
        print '%s mean: %.2f, std: %.2f, nrnull: %s' % (\
            key, \
            results[key][i]['mean'], \
            results[key][i]['std'], \
            results[key][i]['nrnull']
        )
    print ''


        
train = pd.read_csv( "train.csv")
train_dict = {}
for record in train.iterrows():
    record = record[1]
    uid = record['user']
    if uid not in train_dict:
        train_dict[uid] = {}
        
    ts = time.mktime(parse(record['timestamp']).timetuple())
    if ts not in train_dict[uid]:
        train_dict[uid][ts] = []
        
    train_dict[uid][ts].append({
        'eid': record['event'],
        'invited': record['invited'],
        'interested': record['interested'],
        'not_interested': record['not_interested'],
    })

stats = []
for uid, rec_dict in train_dict.iteritems():
    if len(rec_dict) == 1:
        continue
    for ts, recs in rec_dict.iteritems():
        c = len(recs)
        sint = sum([r['interested'] for r in recs])
        snint = sum([r['not_interested'] for r in recs])
        stats.append((uid, c, sint, snint))
        
hist = {}
for stat in stats:
    k = '%s_%s' % (stat[2], stat[3])
    if k not in hist:
        hist[k] = []
    hist[k].append(stat[1])

for key, l in hist.iteritems():
    print key
    print np.mean(l)
    print np.std(l)
    print ''
    
count_hist = {}
for stat in stats:
    k = stat[1]
    if k not in count_hist:
        count_hist[k] = {}
    k2 = '%s_%s' % (stat[2], stat[3])
    count_hist[k][k2] = count_hist[k].get(k2, 0) + 1
