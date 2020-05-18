#!/usr/bin/env python3
import re, json
from datetime import datetime
from urllib.request import urlopen

utc = datetime.strftime(datetime.utcnow(), '%Y-%m-%d %H:%M:%SZ')
URL = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat'
out = 'airports.json'

pattern = re.compile( r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)" )
lines   = urlopen(URL).read().decode().splitlines()
data    = {
            'Source'         : URL,
            'altitude_units' : 'feet',
            'download_date'  : utc,
            'airports'       : {}}
for line in lines:
    info = pattern.split( line );                    # Split on commas outside of quotes
    if (info[5] == '\\N'): continue
    data['airports'][info[5][1:-1]] = {
        'Name'      : info[1][1:-1]  if (info[1] != '\\N') else None,
        'City'      : info[2][1:-1]  if (info[2] != '\\N') else None,
        'Country'   : info[3][1:-1]  if (info[3] != '\\N') else None,
        'IATA'      : info[4][1:-1]  if (info[4] != '\\N') else None,
        'ICAO'      : info[5][1:-1]  if (info[5] != '\\N') else None,
        'latitude'  : float(info[6]) if (info[6] != '\\N') else None,
        'longitude' : float(info[7]) if (info[7] != '\\N') else None,
        'altitude'  : float(info[8]) if (info[8] != '\\N') else None}

with open(out, 'w') as fid:
    json.dump( data, fid, indent = 2 )

