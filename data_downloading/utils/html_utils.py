import logging
import os, time
from threading import Thread, BoundedSemaphore, Semaphore, Lock
from urllib.request import urlopen
from bs4 import BeautifulSoup as BS

BLOCK = 2**20
HOME  = os.path.expanduser('~')

SEMA  = BoundedSemaphore()
LOCK  = Lock()

def rateFMT( num, suffix='B'):
 for unit in ['','K','M','G','T','P','E','Z']:
   if abs(num) < 1024.0:
     return "{:3.1f}{}{}/s".format(num, unit, suffix)
   num /= 1024.0
 return "{:.1f}{}{}/s".format(num, 'Y', suffix);

def updateThreads(threads):
  if not isinstance(threads, int) or threads < 1:
    return																																		# If thread is NOT an integer or less than 1, just return
  with LOCK:																																	# Grab lock, don't want other threads mucking this up
    diff = threads - SEMA._initial_value																			# Compute difference between 
    if diff > 0:																														  # If need to increase thread count	
      SEMA._initial_value = threads																						# Change initial value
      for i in range(diff):																										# Iterate over difference
        SEMA.release()																												# Call release to increase _value
    elif diff < 0:																														# If diff is negative, then have to decrement _initial_value
      for i in range( abs(diff) ):																						# Iterate over absolute value of diff
        SEMA.acquire()																												# Grab object to decrement _value
        SEMA._initial_value -= 1																							# Decrement _initial_value, note we do NOT call a release because we don't want _value going back up

def urlJoin( *args ):
  args = [arg[:-1] if arg[-1] == '/' else arg for arg in args]
  return '/'.join( args )

def urlBase( url ):
  return url.split('/')[-1]

class URLDownloader:
  """Class for opening URL, downloading data, and closing URL"""

  def __init__(self, url):
    self.log  = logging.getLogger(__name__)
    self.url  = url
    self.resp = None
    self.size = None

  def __enter__(self):
    """Open a URL using the with statement"""

    self.open()																																# Run open method
    return self																																# Return self

  def __exit__(self, *args):
    """Close the URL when used in a with statement"""

    self.close()																															# Run close method

  def open(self):
    """Actually open the URL"""

    try:
      self.resp = urlopen( self.url )
    except Exception as err:
      self.log.error( 'Failed to open URL: {}'.format(err) )
      return False
    else:
      self.size = int( self.resp.getheader('Content-Length', 0) )
      return True

  def close(self):
    """Try to close the URL cleanly"""

    try:
      self.resp.close()
    except:
      pass

  def checkSize(self, size):
    """
    Check size against remote

    Arguments:
      size (int): Check if remote file is this size

    Keyword arguments:
      None

    Returns:
      True if sizes match, False if mismatch, None if something is None

    """

    if isinstance(self.size, int) and isinstance(size, int):									# If size attribute and size are integers
      return self.size == size																								# Check if same
    return None																																# Return None

  def read(self, *args, **kwargs):
    """
    Read bytes from the URL

    Arguments:
      *args: Any argument accepted by HTTPResponse.read

    Keyword arguments:
      **kwargs: Any keyword argument accepted by HTTPResponse.read

    Returns:
      bytes: Data read from remote

    """

    if self.resp:
      try:
        return self.resp.read(*args, **kwargs)
      except Exception as err:
        self.log.error( 'Failed to read data from URL: {}'.format(err) )
    return None

  def _toFile(self, fPath, blocksize):
    """
    Download data in chunks and write to file

    Arguments:
      fPath (str): Path to local file to save data
      blocksize (int): Download chunk size

    Keyword arguments:
      None.

    Returns:
      bool: True if download success, False otherwise.

    """
    t0     = time.time()
    dlSize = 0																																# Size of downloaded data
    os.makedirs( os.path.dirname(fPath), exist_ok = True )										# Make directory path
    with open(fPath, 'wb') as fid:																						# Open local file in binary write
      data = self.read( blocksize )																						# Read chunk from remote
      while data:																															# While chunk has data
        dlSize += fid.write( data )																						# Write chunk to file and increment dlSize based on number of bytes written
        data = self.read( blocksize )																					# Read another chunk from remote

    if self.checkSize(dlSize):																								# If dlSize matches the remote size, then we got all the data
      dlr = rateFMT( dlSize / (time.time()-t0) )
      self.log.info('Downloaded : {} at {}'.format(self.url, dlr))		    # Log some info
      return True																															# Return True
    return False																															# If got here, something failed, return False

  def download(self, fPath = None, overwrite = False, blocksize = BLOCK):
    if fPath:
      fSize = os.stat(fPath).st_size if os.path.isfile(fPath) else None
    else:
      fSize = None

    if not overwrite and self.checkSize(fSize):
      self.log.info('File already downloaded; set overwrite: {}'.format(self.url))
      return True

    if fPath:
      return self._toFile(fPath, blocksize)
    else:
      return self.read()

    self.log.warning('Download failed!')
    return False

def download(url, **kwargs):
  """
  Download and return bytes from URL

  Arugments:
    url (str): URL to download bytes from

  Keyword arguments:
    **kwargs

  Returns:
    bytes, bool: If no fPath was given, returns bytes on successful
      download. If fPath was given, returns True on success. False
      if failed in either case

  """

  with SEMA:
    with URLDownloader(url) as dl:
      return dl.download( **kwargs )

def threadedDownload(url, **kwargs):
  """
  Use for parallel downloads; wrapper for download function

  This function acts as a wrapper for the download function.
  First grab a Semaphore object to ensure there aren't too many
  operations happening at once, then run the download function
  in a new thread.

  Note:
    This function will block if there are too many operations 
    occurring; i.e., could not acquire the Semaphore.

  Arugments:
    url (str): URL to download bytes from

  Keyword arguments:
    **kwargs

  Returns:
    None

  """

  with SEMA:																																	# Grab semaphore lock so we don't open too many threads
    t = Thread(target=download, args=(url,), kwargs=kwargs)										# Set up a thread to download
    t.start()																																	# Start the thread
 
def getBeautifulSoup(url, features = 'lxml', **kwargs):
  """
  Download data from URL and parse with bs4.BeautifulSoup

  Arguments:
    url (str): URL to download data from

  Keyword arguments:
    features (str): Desirable features of the parser.
      See bs4.BeautifulSoup for more information

  Returns:
    A bs4.BeautifulSoup object of parsed HTML data

  """

  data = download( url )
  if data:
    return BS(data, features)
  return None

def getHREF( URL, ext = None, **kwargs ):
  """Generator to produce hyper-references from URL"""

  data = getBeautifulSoup( URL, **kwargs )																							
  if data:
    for a in data.find_all('a', href=True):
      if a.parent.name == 'th':
        continue
      href = a.get('href')
      if href in URL:
        continue
      elif href[-1] == '/':
        yield from getHREF( urlJoin(URL, href), ext = ext, **kwargs )
      elif ext and href.endswith( ext ):
        yield urlJoin(URL, href)
      else:
        yield urlJoin(URL, href)
  yield None

def htmlTableIterator( bsData ):
  """
  Iterator to yield HTML table rows

  This function with iterate over all tables in the object, then iterate over
  all rows in a given table.

  Arguments:
    bsData (bs4.BeautifulSoup): A BeautifulSoup object

  Keyword arguments:
    None

  Returns:
    Yields HTML table row, None if not tables found

  """

  tables = bsData.find_all('table')																						# Find all tables in the data

  if len(tables) > 0:																													# If tables found
    for table in tables:																											# Iterate over all tables
      for row in table.find_all('tr'):																				# Iterate over all rows in the table
        yield row.find_all('td')																															# Yield the row
  else:																																				# Else
    yield None																																# Yield None
