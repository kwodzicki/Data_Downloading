import logging
import os
from datetime import datetime, timedelta
from ..utils.html_utils import urlJoin, getHREF, threadedDownload, updateThreads

BASEURL = 'https://www1.ncdc.noaa.gov/pub/data/cmb/ersst'
DATEFMT = '%Y%m'

def getERSST(outRoot, version = '5', startDate = None, endDate = None, netCDF = True, **kwargs):
  log = logging.getLogger(__name__)
  updateThreads( kwargs.get('threads', 4) )

  dataType = 'netcdf' if netCDF else 'ascii'
  version  = 'v{}'.format(version)
  URL      = urlJoin(BASEURL, version, dataType)
  outDir   = os.path.join(outRoot, 'ERSST', version)

  for url in getHREF( URL ):
    if url:
      try:
        date = datetime.strptime(url.split('.')[-2], DATEFMT)
      except:
        continue
      else:
        ref = url.split('/')
      if startDate and date < startDate:
        continue
      if endDate and date > endDate:
        continue
      kwargs['fPath'] = os.path.join( outDir, ref[-1] )
      threadedDownload(url, **kwargs)
