from math import isnan, sqrt

import pandas as pd


user_info_file = pd.read_csv("users.csv")
loc_dict = {}
for user in user_info_file.iterrows():
    user = user[1]
    uid = int(user['user_id'])
    if isinstance(user['location'], str):
        tokens = user['location'].split('  ')
        if not tokens:
            continue
        filtered = []
        for t in tokens:
            if t:
                try:
                    t = int(t)
                    continue
                except:
                    pass
                if t == 'undefined':
                    continue
                t = t.strip()
                filtered.append(t)
        if not filtered:
            continue
        loc_dict[uid] = {
            'tokens': filtered,
        }

loc_dict2 = {}
chunks = pd.read_csv("events.csv", iterator=True, chunksize=10000)
latlngdict = {}
count = 0
for chunk in chunks:
    for e in chunk.iterrows():
        e = e[1]
        eid = int(e['event_id'])
        city = e['city']
        state = e['state']
        country = e['country']
        lat = e['lat']
        lng = e['lng']
        if isinstance(city, float):
            city = None
        if isinstance(state, float):
            state = None
        if isinstance(country, float):
            country = None
        if isnan(lat) or isnan(lng):
            lat = None
            lng = None
        if not (city or state or country or lat or lng):
            continue
        d = {}
        if city:
            d['city'] = city
        if state:
            d['state'] = state
        if country:
            if country == 'Democratic Republic Congo':
                country = 'Democratic Republic of the Congo'
            d['country'] = country
        d['lat'] = lat
        d['lng'] = lng
        if lat:
            if city:
                latlngdict[(lat, lng)] = (city, country, state)
            else:
                count += 1
        loc_dict2[eid] = d

# approximate coordinates a little, for faster search
latlngaprox = {}
for coords, l in latlngdict.iteritems():
    lat = int(coords[0] * 5) / 5.0
    lng = int(coords[1] * 5) / 5.0
    c2 = (lat, lng)
    if c2 not in latlngaprox:
        latlngaprox[c2] = set([])
    if l not in latlngaprox[c2]:
        latlngaprox[c2].add(l)
        
# invert dictionary
locationlatlng = {}
for coords, ls in latlngaprox.iteritems():
    for l in ls:
        locationlatlng[l] = coords
        
# match lag lng pairs without city location
event_location = {}
count = len(loc_dict2)
missed = 0
for eid, edict in loc_dict2.iteritems():
    count -= 1
    if count % 10000 == 0:
        print count
    lmin = None
    if edict['lat'] and edict['lng']:
        ecoord = (int(edict['lat'] * 5) / 5.0, int(edict['lng'] * 5) / 5.0)
        if ecoord in latlngaprox:
            lmin = latlngaprox[ecoord]
        else:
            dmin = 5
            vecinity = [i / 5.0 for i in range(-10, 11, 2)]
            for i in vecinity:
                for j in vecinity:
                    coords = (ecoord[0] + i, ecoord[1] + j)
                    if coords in latlngaprox:
                        dist = sqrt((coords[0] - edict['lat'])**2 + \
                                (coords[1] - edict['lng'])**2)
                        if dist < dmin:
                            dmin = dist
                            lmin = [l]
    if not lmin:
        l = (edict.get('city'), edict.get('country'), edict.get('state'))
        if l in locationlatlng:
            coords = locationlatlng[l]
            lmin = latlngaprox[coords]
        else:
            lmin = [l]
            print l
            missed += 1
    event_location[eid] = lmin
    
    
countries = set([])
for vl in event_location.itervalues():
    for v in vl:
        if v[1]:
            countries.add(v[1])
        
cities = {c: set([]) for c in countries}
for vl in event_location.itervalues():
    for v in vl:
        if v[0] and v[1]:
            cities[v[1]].add(v[0])

us_states = {'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado','CT':'Connecticut','DE':'Delaware','FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa','KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan','MN':'Minnesota','MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada','NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma','OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont','VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming'}
us_states_rev = {v:k for k,v in us_states.iteritems()}
canada_states = {'AB':'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba', 'NB': 'New Brunswick', 'NL': 'Newfoundland', 'NT': 'Northwest Territories', 'NS': 'Nova Scotia', 'NU': 'Nunavut', 'ON': 'Ontario', 'PE': 'Prince Edward Island', 'QC': 'Quebec', 'SK': 'Saskatchewan', 'YT': 'Yukon'}
canada_states_rev = {v:k for k,v in canada_states.iteritems()}
australia_states = ['Australian Capital Territory', 'New South Wales', 'Northern Territory', 'Queensland', 'South Australia', 'Tasmania', 'Victoria', 'Western Australia']

# set up a city: country dictionary
cities_map = {}
for country, cities_set in cities.iteritems():
    for city in cities_set:
        if city not in cities_map:
            cities_map[city] = {}
        cities_map[city][country] = cities_map[city].get(country, 0) + 1
        
        

# process user location tokens
for uid, udict in loc_dict.iteritems():
    if 'tokens' not in udict:
        continue
    tokens = udict['tokens']
    country = None
    state = None
    city = None
    
    for t in tokens:
        if t in countries:
            country = t
            break
            
    for t in tokens:
        if t in us_states:
            state = t
            country = 'United States'
        if t in us_states_rev:
            state = us_states_rev[t]
            country = 'United States'
        if t in canada_states:
            country = 'Canada'
            state = t
        if t in canada_states_rev:
            country = 'Canada'
            state = canada_states_rev[t]
        if t in australia_states:
            state = t
            country = 'Australia'

    if country:
        for t in tokens:
            if t in cities[country]:
                city = t
    
    if not (city or state or country):
        country_set = set(countries)
        for t in tokens:
            if t in cities_map:
                
                to_remove = ['United States', 'Canada', 'Australia']
                i = 0
                while len(cities_map[t]) > 1 and i < 3:
                    if to_remove[i] in cities_map[t]:
                        cities_map[t].pop(to_remove[i])
                    i += 1
                    
                if len(cities_map[t]) == 1:
                    city = t
                    country = cities_map[t].keys()[0]
                elif len(cities_map[t]) > 1:
                    country_set.intersection_update(cities_map[t])
        if not country and len(country_set) < 5:
            country = list(country_set)

    if not (city or state or country):
        country = tokens
        
        
    if state:
        udict['state'] = state
    if country:
        udict['country'] = country
    if city:
        udict['city'] = city
        
count = 0
for uid, udict in loc_dict.iteritems():
    state = udict.get('state')
    country = udict.get('country')
    city = udict.get('city')
    if isinstance(country, str):
        country = [country]
    locations = []
    for c in country:
        l = {'country': c}
        if state:
            l['state'] = state
        if city:
            l['city'] = city
        locations.append(l)
    udict['locations'] = locations

count = 0
for uid, udict in loc_dict.iteritems():
    count += 1
    if count % 1000 == 0:
        print count
    user_info.update({'id': uid},
        {'$set': {'newloc2': udict['locations']}})

count = 0
event_info2 = db.event_info
event_info2.ensure_index('id', {'unique': True})
for e in event_info.find():
    eid = e['id']
    locs = event_location.get(eid, [])
    count += 1
    if count % 10000 == 0:
        print count
    filtered = set([])
    for l in locs:
        state = us_states_rev.get(l[2], l[2])
        state = canada_states_rev.get(state, state)
        filtered.add((l[0], l[1], state))
    filtered = [{'city': l[0], 'country': l[1], 'state': l[2]} \
        for l in filtered]
    e['newloc2'] = filtered
    event_info2.insert(e)
'''
# moving to a new collection
count = 0
for e in event_info.find():
    count += 1
    if count % 10000 == 0:
        print count
    if e['id'] in loc_dict2:
        e['newloc'] = [loc_dict2[e['id']]]
    e.pop('_id')
    event_info2.insert(e)
'''
