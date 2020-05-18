import logging
import os
from datetime import datetime
from ..utils.html_utils import threadedDownload, getHREF, updateThreads, urlBase
BASEURL = 'http://atmos.tamucc.edu/trmm/data/trmm'
DATEFMT = '%Y%m'

def getTRMM_PF(outRoot, level = '2', startDate = None, endDate = None, **kwargs):
  log = logging.getLogger(__name__)
  updateThreads( kwargs.get('threads', 4) )

  level  = 'level_{}'.format(level)
  URL    = '{}/{}'.format(BASEURL, level)
  outDir = os.path.join( outRoot, 'PF', level )

  for url in getHREF(URL):
    if url:
      try:
        date = urlBase(url).split('_')[1].split('.')[0]
        date = datetime.strptime( date, DATEFMT )
      except:
        continue
      else:
        ref = url.split('/')
      if startDate and date < startDate:
        continue
      if endDate and date > endDate:
        continue
      kwargs['fPath'] = os.path.join( outDir, *ref[-2:] )
      threadedDownload(url, **kwargs)
