#!/usr/bin/env python

import os
from datetime import datetime
from bs4 import BeautifulSoup as BS;

try:
  from urllib2.requests import urlopen;
except:
  from urllib2 import urlopen;

outDir = '/Users/kwodzicki/Data/'
url_base = 'http://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/'

URL_date_fmt  = '%Y%m%d%H';
FILE_date_fmt = '%Y%m%d-%HZ';

date = datetime(2018, 10, 1, 12);
hrs  = ['f{:03d}'.format(i) for i in range(0, 49, 6)];

dateSTR = date.strftime(FILE_date_fmt)
outDir  = os.path.join( outDir, 'GFS_{}'.format( dateSTR ) );
url     = url_base + 'gfs.' + date.strftime( URL_date_fmt ) + '/';
if not os.path.isdir(outDir): os.makedirs( outDir );

html = urlopen(url).read();
soup = BS(html, 'lxml');
for link in soup.find_all('a'):
  if 'gfs.t12z.pgrb2.1p00.' in link.text and '.idx' not in link.text:
    if any( [hr in link.text for hr in hrs] ):
      url_file = url + link.text;
      f_hour   = link.text.split('.')[-1]
      out_file = 'gfs_{}_{}.grb'.format( dateSTR, f_hour )
      out_file = os.path.join( outDir, out_file );
      if not os.path.isfile( out_file ):
        with open(out_file, 'wb') as f:
          f.write( urlopen( url_file ).read() );
    
    
  
# for line in lines:
#   if 'gfs.t12z.pgrb2.1p00.' in line
# print(data)
