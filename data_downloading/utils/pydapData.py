import logging
import os, sys
import json
import time

import numpy as np

#from pydap.handlers.dap import DAPHandler
from pydap.client import open_url#, Functions
from pydap.cas.urs import setup_session

HOME = os.path.expanduser('~')
info = os.path.join(HOME, '.earthdataloginrc')
if os.path.isfile(info):
  with open(info, 'rb') as fid:
    info = json.load(fid)
  USER   = info['user']
  PASSWD = info['passwd']
else:
  USER   = None
  PASSWD = None

FAILEDFMT = 'Attempt {:2d} of {:2d} - Failed to get {}'
LITTLEEND = sys.byteorder == 'little'
NATIVE    = LITTLEEND and '<' or '>'
SWAPPED   = LITTLEEND and '>' or '<'


"""
class PyDAPDataset( DAPHandler ):
  def __init__(self, url, **kwargs ):
    session = kwargs.pop('session', None)
    if not session:
      session = setup_session( kwargs.get('username', decoder(USER)), 
                               kwargs.get('password', decoder(PASSWD)), 
                               check_url = url )
    application = kwargs.pop('application', None)
    output_grid = kwargs.pop('output_grid', True) 
    super().__init__(url, application, session, output_grid, **kwargs)
    self.dataset.functions = Functions(url, application, session)

  def __getitem__(self, key):
    if self.dataset:
      return self.dataset.get(key, None)
    return None
      
  def __getattr__(self, key):
    if self.dataset:
      return getattr(self.dataset, key, None)
    return None

  def __contains__(self, key):
    return key in self.dataset
"""


def scaleFillData(data, atts, fillValue = None):
  log = logging.getLogger(__name__);
  if '_FillValue' in atts:
    log.debug( 'Searching for Fill Values in data' );
    bad = (data == atts['_FillValue'])
  else:
    log.debug( "No '_FillValue' attribute" );
    bad = None
  if 'missing_value' in atts:
    log.debug( 'Searching for missing values in data' );    
    if bad is None:
      bad = (data == atts['missing_value']);
    else:
      bad = ((data == atts['missing_value']) | bad);
  else:
    log.debug( "No 'missing_value' attribute" );

  if 'scale_factor' in atts and 'add_offset' in atts:
    log.debug( "Scaling to data" );
    data = data * atts['scale_factor'] + atts['add_offset']
  if bad is not None:
    if np.sum(bad) > 0:
      log.debug( 'Replacing missing and fill values with NaN characters' );
      if data.dtype.kind != 'f':
        log.debug( 'Converting data to floating point array' );
        data = data.astype(np.float32);
  data[bad] = np.nan if fillValue is None else fillValue;
  return data   

class PyDAPDataset():

  def __init__(self, url, **kwargs):
    self._session = None
    self._dataset = None

    self.log      = logging.getLogger(__name__)
   
    self.url      = url
    self.retry    = kwargs.get('retry', 3)
    self.kwargs   = kwargs

    self._initDataset()

  def __getitem__(self, key):
    if self._dataset:
      return self._dataset.get(key, None)
    return None
      
  def __getattr__(self, key):
    if self._dataset:
      return getattr(self._dataset, key, None)
    return None

  def _initSession(self):
    """
    Initiailze pydap session for loading data

    Initialize a session for a data set given a username, password, and
    URL for the data. Any previously open sessions are closed

    """

    self.log.debug( f'Initializing session : {self.url}' )
    try:
      self._session.close()
    except Exception as err:
      self.log.debug( f'Failed to close previous session: {err}' )

    try:
      self._session  = setup_session(
              self.kwargs.get('username', USER), 
              self.kwargs.get('password', PASSWD), 
              check_url = self.url
      )
    except Exception as err:
      self.log.error( f'Failed to start session: {err}' )
      return False

    return True

  def _initDataset(self):
    """Open pydap dataset for reading"""
    
    self.log.debug( f'Loading dataset : {self.url}' )
    try:
      self._dataset.close()
    except Exception as err:
      self.log.debug( f'Failed to close previous dataset: {err}' )
      pass

    if not self._initSession():
      return False

    try:
      self._dataset = open_url( self.url, session = self._session )
    except Exception as err:
      self.log.error( f'Failed to open dataset: {err}' )
      return False

    return True

  def _randomReload(self):
    """
    Reload dataset after random delay
    
    Close remote file, sleep random amount of time between 15 and 30 mintues,
    then reopne remote dataset

    """

    self.log.debug( f'Reloading dataset, closing : {self.url}' )
    self.close()
    dt = float( np.random.random( 1 ) )
    dt = (dt + 0.5) * 1800 
    self.log.debug( 'Sleeping {:4.1f} mintues'.format( dt / 60.0 ) )
    time.sleep( dt )
    self._initDataset()

  def close(self):
    """Safely close remote dataset"""

    try:
      self._session.close()
    except:
      pass
    try:
      self._dataset.close()
    except:
      pass
    self._session = None
    self._dataset = None

  def getVarAtts( self, varName, retry = None ):
    self.log.info( f'Getting attributes : {varName}' )
    if not isinstance(retry, int): retry = self.retry

    attempt = 0
    while attempt < retry:
      attempt += 1
      try:
        atts               = self._dataset[varName].attributes
        atts['dimensions'] = self._dataset[varName].dimensions
      except:
        self.log.warning( FAILEDFMT.format(attempt, retry, 'attributes') )
        self._randomReload()
      else:
        return atts
    return None

  def getValues( self, varName, slices = None, retry = None ):
    self.log.info( f'Getting data : {varName}' )
    if not isinstance(retry, int): retry = self.retry
    attempt = 0
    dims    = self._dataset[varName].shape
    if slices is None:
      slices = []
      for i in range( len(dims) ):
        slices.append( slice(0, dims[i]) )
      slices = tuple( slices )
  
    while attempt < retry:
      attempt += 1
      try:
        values = self._dataset[varName].data[ slices ]
      except:
        self.log.warning(FAILEDFMT.format(attempt, retry, 'data') )
        self._randomReload()
      else:
        return values
    return None     

  def getVar( self, varName, slices = None, scaleandfill = False):
    atts = self.getVarAtts( varName )
    if atts is not None:
      values = self.getValues( varName, slices = slices)
      if values is not None:
        if values.dtype.byteorder == SWAPPED:
          dt     = np.dtype( str(values.dtype).replace(SWAPPED, NATIVE)  )
          values = values.astype( dt )
        if scaleandfill:
          return scaleFillData(values, atts), atts
        else:
          return values, atts
    return None, None

