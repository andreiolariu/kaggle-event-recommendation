import pandas as pd
from math import isnan

user_data = loc_dict

user_info_file = pd.read_csv("users.csv")
for user in user_info_file.iterrows():
    user = user[1]
    uid = int(user['user_id'])
    d = {}
    if 'locale' in user and not isinstance(user['locale'], float):
        d['locale'] = user['locale']
    if 'timezone' in user and not isnan(user['timezone']):
        d['tz'] = user['timezone']
    if uid not in user_data:
        user_data[uid] = {}
    user_data[uid].update(d)

#for user in user_info.find():
#    user_data[user['id']]['locations'] = user.get('newloc', [])

vote_locale = {}
countries = set([])
for user in user_data.itervalues():
    if 'locale' in user and 'locations' in user:
        locale = user['locale']
        if locale not in vote_locale:
            vote_locale[locale] = {}
        for l in user['locations']:
            country = l['country']
            countries.add(country)
            vote_locale[locale][country] = vote_locale[locale].get(country, 0) + 1
            
locales = {'es_NI': 'Nicaragua', 'tr_TR': 'Turkey', 'en_SG': 'Singapore', 'th_TH': 'Thailand', 'es_VE': 'Venezuela', 'hu_HU': 'Hungary', 'es_AR': 'Argentina', 'ar_EG': 'Egypt', 'is_IS': 'Iceland', 'zh_HK': 'Hong Kong', 'de_AT': 'Austria', 'pt_BR': 'Brazil', 'cs_CZ': 'Czech Republic', 'sk_SK': 'Slovakia', 'mk_MK': 'Macedonia', 'ar_MA': 'Morocco', 'en_ZA': 'South Africa', 'sv_SE': 'Sweden', 'in_ID': 'Indonesia', 'es_PR': 'Puerto Rico', 'sr_ME': 'Montenegro', 'fr_FR': 'France', 'fi_FI': 'Finland', 'et_EE': 'Estonia', 'sr_RS': 'Serbia', 'es_PY': 'Paraguay', 'no_NO': 'Norway', 'nl_NL': 'Netherlands', 'es_PE': 'Peru', 'lv_LV': 'Latvia', 'es_PA': 'Panama', 'el_CY': 'Cyprus', 'ro_RO': 'Romania', 'iw_IL': 'Israel', 'es_CO': 'Colombia', 'es_CL': 'Chile', 'es_CR': 'Costa Rica', 'hr_HR': 'Croatia', 'ru_RU': 'Russia', 'da_DK': 'Denmark', 'ar_LB': 'Lebanon', 'sq_AL': 'Albania', 'ms_MY': 'Malaysia', 'ar_OM': 'Oman', 'es_HN': 'Honduras', 'pt_PT': 'Portugal', 'vi_VN': 'Vietnam', 'en_NZ': 'New Zealand', 'ar_YE': 'Yemen', 'ar_SD': 'Sudan', 'be_BY': 'Belarus', 'sr_CS': 'Serbia and Montenegro', 'ar_BH': 'Bahrain', 'ar_JO': 'Jordan', 'es_EC': 'Ecuador', 'hi_IN': 'India', 'ja_JP': 'Japan', 'lt_LT': 'Lithuania', 'sl_SI': 'Slovenia', 'es_ES': 'Spain', 'en_GB': 'United Kingdom', 'bg_BG': 'Bulgaria', 'es_SV': 'El Salvador', 'zh_TW': 'Taiwan', 'sr_BA': 'Bosnia and Herzegovina', 'ar_AE': 'United Arab Emirates', 'es_BO': 'Bolivia', 'zh_CN': 'China', 'it_CH': 'Switzerland', 'ar_IQ': 'Iraq', 'ar_QA': 'Qatar', 'ar_SA': 'Saudi Arabia', 'ar_LY': 'Libya', 'it_IT': 'Italy', 'uk_UA': 'Ukraine', 'el_GR': 'Greece', 'ar_SY': 'Syria', 'fr_BE': 'Belgium', 'ar_DZ': 'Algeria', 'ga_IE': 'Ireland', 'es_GT': 'Guatemala', 'en_AU': 'Australia', 'ar_TN': 'Tunisia', 'es_UY': 'Uruguay', 'en_PH': 'Philippines', 'mt_MT': 'Malta', 'es_US': 'United States', 'ko_KR': 'South Korea', 'de_LU': 'Luxembourg', 'de_DE': 'Germany', 'es_MX': 'Mexico', 'fr_CA': 'Canada', 'es_DO': 'Dominican Republic', 'pl_PL': 'Poland', 'ar_KW': 'Kuwait'}
locales.update({
    'af_ZA': 'South Africa',
    'cy_GB': 'United Kingdom',
    'bn_IN': 'India',
    'ca_ES': 'Spain',
    'az_AZ': 'Azerbaijan',
    'id_ID': 'Indonesia',
    'ka_GE': 'Georgia',
    'km_KH': 'Cambodia',
    'pa_IN': 'India',
    'ku_TR': 'Turkey',
    'en_IN': 'India',
    'he_IL': 'Israel',
    'bs_BA': 'Bosnia and Herzegovina',
    'fa_IR': 'Iran',
    'mn_MN': 'Mongolia',
    'tl_PH': 'Philippines',
    'nb_NO': 'Norway',
    'jv_ID': 'Indonesia',
})

'''
count = 0
for uid, user in user_data.iteritems():
    if 'locale' not in user:
        continue
    if user['locale'] not in locales:
        continue
    locale_country = locales[user['locale']]
    locs = user.get('locations', [])
    sw = 0
    for loc in locs:
        if loc['country'] == locale_country:
            sw = 1
    if sw == 0:
        locs.append({'country': locale_country})
        user_data[uid]['locations'] = locs
        count += 1

count = 0
for user in user_data.itervalues():
    if 'locations' not in user or not user['locations']:
        print user
        count += 1
'''    
timezones = {}
for user in user_data.itervalues():
    if 'tz' not in user or 'locations' not in user or not user['locations']:
        continue
    if user['tz'] not in timezones:
        timezones[user['tz']] = {}
    tz = timezones[user['tz']]
    for loc in user['locations']:
        tz[loc['country']] = tz.get(loc['country'], 0) + 1

# prune timezone countries
for tz in timezones.keys():
    countries = timezones[tz]
    countries = sorted(countries.items(), key=lambda x: -x[1])
    s = sum([c[1] for c in countries])
    if s < 5:
        timezones[tz] = {}
        continue
    i = 1
    while i < len(countries) and countries[i][1] > s / 5.0:
        i += 1
    countries = dict(countries[:i])
    timezones[tz] = countries
    
count = 0
for uid in user_data.keys():
    user = user_data[uid]
    if 'tz' not in user or 'locale' not in user:
        continue
    tz_data = timezones[user['tz']]
    if user['locale'] not in locales:
        continue
    locale_country = locales[user['locale']]
    if locale_country not in tz_data.keys():
        continue
    sw = 0
    for loc in user.get('locations', []):
        if locale_country == loc['country']:
            sw = 1
    if sw == 0:
        if 'locations' not in user:
            user['locations'] = []
        user['locations'].append({'country': locale_country})
        count += 1   
            
friends = {}
user_ids = set(user_data.keys())
count = 0
for f in friends_db.find():
    count += 1
    if count % 1000 == 0:
        print count
    friends[f['uid']] = user_ids.intersection(f['friends'])

count = 0
for uid in user_ids:
    # collect locations from friends
    friend_votes = {}
    for fid in friends.get(uid, []):
        if 'locations' not in user_data[fid]:
            continue
        flocs = user_data[fid]['locations']
        for loc in flocs:
            key = '%s-%s-%s' % (loc.get('country'),loc.get('state'),loc.get('city')) 
            if key not in friend_votes:
                friend_votes[key] = {
                    'location': copy.copy(loc),
                    'vote': 1
                }
            else:
                friend_votes[key]['vote'] += 1
    
    # filter
    friend_votes = friend_votes.values()
    s = sum([x['vote'] for x in friend_votes])
    friend_votes = [f for f in friend_votes if f['vote'] > 1 and f['vote'] > s  / 5]
    if not friend_votes:
        continue
    
    # merge with user's locations
    if 'locations' not in user_data[uid]:
        user_data[uid]['locations'] = []
    for floc in friend_votes:
        sw = 0
        floc = floc['location']
        for uloc in user_data[uid]['locations']:
            if floc['country'] == uloc['country'] and \
                    floc.get('state') ==  uloc.get('state') and \
                    floc.get('city') ==  uloc.get('city'):
                sw = 1
        if sw == 0:
            user_data[uid]['locations'].append(floc)
            count += 1
    
count = 0
for uid, data in user_data.iteritems():
    count += 1
    if count % 1000 == 0:
        print count
    user_info.update({'id': uid},
        {'$set': {'newloc': data['locations']}})
    
## --- finished with users
## now let's do the events
    
# get events
'''
event_data = {}
for e in event_info.find():
    if 'newloc' not in e or not e['newloc']:
        event_data[e['id']] = []
'''
event_data = loc_dict2
    
# get attendance:
attended = {}
count = attendance.count()
for att in attendance.find():
    count -= 1
    if count % 10000 == 0:
        print count
    if att['eid'] not in event_data:
        continue
    if att['uid'] not in user_ids:
        continue
    if 'yes' not in att and 'maybe' not in att:
        continue
    eid = att['eid']
    if eid not in attended:
        attended[eid] = []
    attended[eid].append(att['uid'])
    
# get location from attendants
count = 0
for e in event_data.iterkeys():
    uids = attended.get(e)
    if not uids:
        continue
    votes = {}
    for uid in uids:
        locs = user_data[uid]['locations']
        for loc in locs:
            key = '%s-%s-%s' % (loc.get('country'),loc.get('state'),loc.get('city')) 
            if key not in votes:
                votes[key] = {
                    'location': copy.copy(loc),
                    'vote': 1
                }
            else:
                votes[key]['vote'] += 1
    
    # filter
    votes = votes.values()
    s = sum([x['vote'] for x in votes])
    votes = [f for f in votes if f['vote'] > s / 5]
    
    elocs = [v['location'] for v in votes]
    count += 1
    user_info.update({'id': e},
        {'$set': {'newloc': elocs}})
    

#- --- get location from friends not in user db
# create dict for new users
friend_loc = {}
count = attendance.count()
for att in attendance.find():
    count -= 1
    if count % 10000 == 0:
        print count
    if att['eid'] not in event_data:
        continue
    if att['uid'] in user_ids:
        continue
    if not ('yes' in att or 'maybe' in att or 'interested' in att):
        continue
    friend_loc[att['uid']] = []
    
# find known friends
for f in friends_db.find():
    uid = f['uid']
    for fuid in f['friends']:
        if fuid in friend_loc:
            friend_loc[fuid].append(uid)
            
keys = friend_loc.keys()
for k in keys:
    if not friend_loc[k]:
        friend_loc.pop(k)

friend_locs_dict = {}
for fuid in friend_loc.iterkeys():
    votes = {}
    for uid in friend_loc[fuid]:
        locs = user_data[uid]['locations']
        for loc in locs:
            key = '%s-%s-%s' % (loc.get('country'),loc.get('state'),loc.get('city')) 
            if key not in votes:
                votes[key] = {
                    'location': copy.copy(loc),
                    'vote': 1
                }
            else:
                votes[key]['vote'] += 1
    
    # filter
    votes = votes.values()
    s = sum([x['vote'] for x in votes])
    votes = [f for f in votes if f['vote'] > s / 5]
    
    elocs = [v['location'] for v in votes]
    friend_locs_dict[fuid] = elocs
    
update_based_on = {}
count = attendance.count()
for att in attendance.find():
    count -= 1
    if count % 10000 == 0:
        print count
    if att['eid'] not in event_data:
        continue
    if att['uid'] not in friend_locs_dict:
        continue
    if not ('yes' in att or 'maybe' in att or 'interested' in att):
        continue
    if att['eid'] not in update_based_on:
        update_based_on[att['eid']] = []
    update_based_on[att['eid']].append(att['uid'])


# get location from attendants
count = 0
for e, uids in update_based_on.iteritems():
    count += 1
    if count % 1000 == 0:
        print count
    if not uids:
        continue
    votes = {}
    for uid in uids:
        locs = friend_locs_dict[uid]
        for loc in locs:
            key = '%s-%s-%s' % (loc.get('country'),loc.get('state'),loc.get('city')) 
            if key not in votes:
                votes[key] = {
                    'location': copy.copy(loc),
                    'vote': 1
                }
            else:
                votes[key]['vote'] += 1
    
    # filter
    votes = votes.values()
    s = sum([x['vote'] for x in votes])
    votes = [f for f in votes if f['vote'] > s / 5]
    
    elocs = [v['location'] for v in votes]
    event_info.update({'id': e},
        {'$set': {'newloc': elocs}})
