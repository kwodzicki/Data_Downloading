#!/usr/bin/env python3
import logging
import os, re;
from datetime import datetime
from threading import Thread, Lock, Event
from urllib.request import urlopen
from urllib.parse import urljoin, urlsplit
from bs4 import BeautifulSoup as BS;
import signal
maxAttempt = 3;
outDir     = '/Volumes/flood3/RSS'
bs4FMT     = 'lxml'
urlBase    = 'http://data.remss.com'

URLs       = {'gmi'   : urljoin( urlBase, '{}/bmaps_v{:04.1f}/'    ), 
              'tmi'   : urljoin( urlBase, '{}/bmaps_v{:04.1f}/'    ),
              'amsre' : urljoin( urlBase, '{}/bmaps_v{:02.0f}/'    ),
              'ssmi'  : urljoin( urlBase, '{}/{}/bmaps_v{:02.0f}/' )}

EXT        = '.gz'

log = logging.getLogger(__name__)
log.setLevel( logging.DEBUG )
log.addHandler( logging.StreamHandler() )
log.handlers[0].setLevel( logging.INFO )
log.handlers[0].setFormatter(
        logging.Formatter( '[%(levelname)-4.4s] %(message)s' ) 
)

STOP = Event()

def sigHandler(*args, **kwargs):
  STOP.set()

signal.signal( signal.SIGTERM, sigHandler )
signal.signal( signal.SIGINT,  sigHandler )

class NLock( object ):
  __n    = 0
  __nMax = 0
  def __init__(self, nMax = 2):
    self.__lock1 = Lock()
    self.__lock2 = Lock()
    self.nMax    = nMax
  @property
  def n(self):
    return self.__n
  @property
  def nMax(self):
    return self.__nMax
  @nMax.setter
  def nMax(self, val):
    if isinstance(val, int):
      with self.__lock1:
        self.__nMax = 1 if (val < 1) else val

  def __enter__(self, *args, **kwargs):
    self.acquire( *args, **kwargs )

  def __exit__(self, *args, **kwargs):
    self.release(*args, **kwargs)

  def acquire(self, *args, **kwargs):
    with self.__lock1:
      self.__n += 1
      check     = self.__n >= self.__nMax
      if check:
        self.__lock2.acquire()

  def release(self, *args, **kwargs):
    with self.__lock1:
      self.__n -= 1
      if self.__lock2.locked():
        self.__lock2.release()

class RemoteFile(Thread):
  DAILY    = False
  DAY3     = False
  WEEKLY   = False
  MONTHLY  = False
  DATE     = None
  RESP     = None
  SIZE     = None
  def __init__(self, URL):
    super().__init__()
    self.log       = logging.getLogger(__name__)
    self.URL       = URL
    self.file      = URL.split('/')[-1]
    self._filePath = None
    fileInfo  = self.file.split('_')

    if (len(fileInfo) == 3) and ('d3d' in fileInfo[-1]):              # Then is a 3-day file
      self.DAY3 = True
    elif ('weeks' in self.URL):
      self.WEEKLY = True

    self.date = self._parseDate( fileInfo[1].split('v')[0] )


  def checkDate(self, **kwargs):
    if not self.date: return False																	# If date not defined, return False
    if isinstance(kwargs.get('start', None), datetime) and (self.date < kwargs['start']):
      return False
    if isinstance(kwargs.get('end',   None), datetime) and (self.date >= kwargs['end']):
      return False

    status = []
    if kwargs.get('daily', False):
      status.append( self.DAILY )
    if kwargs.get('day3', False):
      status.append( self.DAY3 )
    if kwargs.get('weekly', False):
      status.append( self.WEEKLY )
    if kwargs.get('monthly', False):
      status.append( self.MONTHLY )

    if len(status) == 0: status.append(True)
    return any( status )

  def localPath(self, outDir):
    remotePath = urlsplit( self.URL ).path
    localPath  = remotePath[1:].replace('/', os.path.sep)
    return os.path.join( outDir, localPath )

  def download( self, outDir = None, filePath = None ):
    if not outDir and not filePath:
      raise Exception('Must enter output directory or file path')
    elif not filePath:
      self._filePath = self.localPath( outDir )
    else:
      self._filePath = filePath
    self.start()
    self.join()
 
  def run(self):
    if self.open(): 
      if self.getSize() and not self._checkSize(self._filePath):
        try:
          data = self.RESP.read()
        except:
          self.error('Failed to download data: {}'.format(self.URL))
        else:
          with open(self._filePath, 'wb') as fid:
            fid.write( data )
      self.close() 

  def open(self):
    if not self.RESP:
      try:
        self.RESP = urlopen( self.URL )
      except:
        self.log.error('Failed to open URL: {}'.format(self.URL))
        return False
    return True

  def close(self):
    if self.RESP:
      try:
        self.RESP.close()
      except:
        self.log.warning('Failed to close remote: {}'.format(self.URL))
        return False
    return True

  def getSize(self):
    if self.open():
      try:
        self.SIZE = int( self.RESP.getheader('Content-Length') )
      except:
        self.log.error('Failed to get remote file size: {}'.format(self.URL))
      else:
        return True
    return False

  def _checkSize( self, filePath ):
    if os.path.isfile( filePath ):
      localSize = os.stat( filePath ).st_size
      if (localSize == self.SIZE):
        log.info('Local file exists and is same size, skipping download: {}'.format(self.URL))
        return True
      else:
        log.info('Local file exists but wrong size, will redownload: {}'.format(self.URL))
    else:
      os.makedirs( os.path.dirname(filePath), exist_ok = True )
      log.info('Local file not exist, will download: {}'.format(self.URL))
    return False

  def _parseDate(self, date):
    try:
      date = datetime.strptime(date, '%Y%m')							# Try to parse Year/month from string
    except:
      pass 
    else:
      self.MONTHLY = True																											# On success, set MONTHLY flag
    if isinstance(date, str):																									# If date is still string instance, then try to parse year/month/day
      try:
        date = datetime.strptime(date, '%Y%m%d')															# Parse year/month/day
      except:
        log.error('Failed to parse date from URL: {}'.format(self.URL))				# Log issue
        return None																														# Return None
    self.DAILY = (not self.MONTHLY) and (not self.WEEKLY) and (not self.DAY3)
    return date

def scraper( URL, ext = EXT, **kwargs ):
  '''
  Purpose:
    Generator function to dive through all directories until
    path with given extension is found. Function is recursive
    will call itself into directories on remote, only yielding
    value when path ends in extension
  Inputs:
    URL     : URL to hunt for data files in
  Keywords:
    ext     : File extension to look for
    Various other keys for filtering by daily, 3-day, weekly,
    and monthly files, and for start/end date filtering.
    See full keyword names in ags list.
  '''
  log = logging.getLogger(__name__)
  html  = urlopen( URL ).read()
  parse = BS(html, bs4FMT)
  path  = urlsplit( URL ).path																								# Path in input URL
  for link in parse.find_all('a'):																						# Loop over all links
    log.debug(  link['href'] )																								# Debug
    if path in link['href']:																									# If path from input URL is in href path, then is file in current directory or directory; used to filter [To Parent] link
      URL = urljoin( URL, link['href'] )																			# Update URL with new path
      log.debug( URL )																												# Log url
      if link.text.endswith( ext ):																						# If the URL ends with requested extension
        remote = RemoteFile(URL)																							# Initialize RemoteFile object
        if remote.checkDate(**kwargs):																				# If date checks out
          yield remote																												# Yield RemoteFile object
      else:																																		# Else, assume is directory
        yield from scraper( URL, ext = ext, **kwargs )												# Recursive call to generator to dive into directory


def downloadFiles(instrument, version, outDir, **kwargs):
  log = logging.getLogger(__name__)
  URL = URLs.get(instrument[0].lower(), None)
  if not URL:
    log.critical( 'Instrument not supported: {}'.format( instrument ) )
    return False

  lock = NLock( kwargs.pop('threads', 2) )
  URL  = URL.format( *instrument, version )

  if isinstance( kwargs.get('start', None), str ):
    kwargs['start'] = datetime.strptime(kwargs['start'], '%Y%m%d')
  if isinstance( kwargs.get('end', None), str ):
    kwargs['end'] = datetime.strptime(kwargs['end'], '%Y%m%d')

  for remote in scraper( URL, **kwargs ):
    with lock:
      remote.download( outDir = outDir )
    if STOP.is_set(): break

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('outdir',     type=str,   help='Output directory for downloaded data')
  parser.add_argument('version',    type=float, help='Data version to download')
  parser.add_argument('instrument', type=str, nargs='+', help='Instrument to download data for. If multiple possible instruments, such as ssmi, enter the sensor name followed by the instrument nubmer; e.g. ssmi f08')
  parser.add_argument('-t',  '--threads', type=int, default=2, help='Number of simultaneous downloads to allow')
  parser.add_argument('-s',  '--start',   type=str, help='ISO 8601 string specifying start date; e.g., 19980101. Start date is inclusive.')
  parser.add_argument('-e',  '--end',     type=str, help='ISO 8601 string specifying end date; e.g., 19980102. End date is exclusive.')
  parser.add_argument('-d',  '--daily',   action='store_true', help='Set to only download daily data')
  parser.add_argument('-d3', '--day3',    action='store_true', help='Set to only download 3-day data')
  parser.add_argument('-w',  '--weekly',  action='store_true', help='Set to only download weekly data')
  parser.add_argument('-m',  '--monthly', action='store_true', help='Set to only download monthly data')
  args = parser.parse_args()

  downloadFiles( args.instrument, args.version, args.outdir,
          threads = args.threads,
          start   = args.start,
          end     = args.end,
          daily   = args.daily,
          day3    = args.day3,
          weekly  = args.weekly,
          monthly = args.monthly)   

