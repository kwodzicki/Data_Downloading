import os
from functools import wraps
from datetime import datetime, timedelta

from .streams import getMerraStream
from .dateutils import next_month

"""
Earth Science Data Type (ESDT)

All CODE2XXXX dictionaries are derived from the MERRA2 data specification
(https://gmao.gsfc.nasa.gov/pubs/docs/Bosilovich785.pdf)
and therefore may not be applicable to other datasets

"""

SWAPKEYVAL = lambda x: {value : key for key, value in x.items()}                # Lambda to swap dictionary key/value pairs

CODE2TYPE = {'M2' : 'MERRA2'}

CODE2TIME = {'I'  : 'INSTANTANEOUS',
             'T'  : 'TIME-AVERAGED',
             'C'  : 'TIME-INDEPENDENT',
             'S'  : 'STATISTICS'}

CODE2FREQ = { 1   : 'HOURLY',
              3   : '3-HOURLY',
              6   : '6-HOURLY',
             'M'  : 'MONTHLY MEAN',
             'D'  : 'DAILY STATISTICS',
             'U'  : 'MONTHLY-DIURNAL MEAN',
              0   : 'NOT APPLICABLE'}

CODE2HRES = {'N'  : 'NATIVE'}

CODE2VRES = {'X'  : 'TWO-DIMENSIONAL',
             'P'  : 'PRESSURE',
             'V'  : 'MODEL LAYER CENTER',
             'E'  : 'MODEL LAYER EDGE'}

CODE2GRUP = {'ANA'  : 'DIRECT ANALYSIS PRODUCTS',
             'ASM'  : 'ASSIMILATED STATE VARIABLES',
             'AER'  :  'AEROSOL MIXING RATIO', 
             'ADG'  :  'AEROSOL EXTENDED DIAGNOSTICS',
             'TDT'  :  'TENDENCIES OF TEMPERATURE', 
             'UDT'  :  'TENDENCIES OF EASTWARD AND NORTHWARD WIND COMPONENTS', 
             'QDT'  :  'TENDENCIES OF SPECIFIC HUMIDITY',
             'ODT'  :  'TENDENCIES OF OZONE', 
             'GAS'  :  'AEROSOL OPTICAL DEPTH', 
             'GLC'  :  'lAND iCE sURFACE', 
             'LND'  :  'LAND SURFACE VARIABLES', 
             'LFO'  :  'LAND SURFACE FORCING OUTPUT',
             'FLX'  :  'SURFACE TURBULENT FLUXES AND RELATED QUANTITIES', 
             'MST'  :  'MOIST PROCESSES',
             'CLD'  :  'CLOUDS', 
             'RAD'  :  'RADIATION',
             'CSP'  :  'cosp SATELLITE SIMULATOR',        
             'TRB'  :  'TURBULENCE',   
             'SLV'  :  'SINGLE LEVEL',
             'INT'  :  'VERTICAL INTEGRALS',
             'CHM'  :  'CHEMISTRY FORCING',
             'OCN'  :  'OCEAN',
             'NAV'  :  'VERTICAL COORDINATES'} 
VARCOORD  = {'longitude' : {'M2' : 'lon'},
             'latitude'  : {'M2' : 'lat'},
             'level'     : {'M2' : 'lev'},
             'time'      : {'M2' : 'time'} }

ESDT2COLL = {'M2C0NXASM' : 'const_2d_asm_Nx', 
             'M2CONXCTM' : 'const_2d_ctm_Nx', 
             'M2CONXLND' : 'const_2d_lnd_Nx', 
             'M2I1NXASM' : 'inst1_2d_asm_Nx', 
             'M2I1NXINT' : 'inst1_2d_int_Nx', 
             'M2I1NXLFO' : 'inst1_2d_lfo_Nx', 
             'M2I3NXGAS' : 'inst3_2d_gas_Nx', 
             'M2I3NVAER' : 'inst3_3d_aer_Nv', 
             'M2I3NPASM' : 'inst3_3d_asm_Np', 
             'M2I3NVASM' : 'inst3_3d_asm_Nv', 
             'M2I3NVCHM' : 'inst3_3d_chm_Nv', 
             'M2I3NVGAS' : 'inst3_3d_gas_Nv', 
             'M2I6NPANA' : 'inst6_3d_ana_Np', 
             'M2I6NVANA' : 'inst6_3d_ana_Nv', 
             'M2SDNXSLV' : 'statD_2d_slv_Nx', 
             'M2T1NXADG' : 'tavg1_2d_adg_Nx', 
             'M2T1NXAER' : 'tavg1_2d_aer_Nx', 
             'M2T1NXCHM' : 'tavg1_2d_chm_Nx', 
             'M2T1NXCSP' : 'tavg1_2d_csp_Nx', 
             'M2T1NXFLX' : 'tavg1_2d_flx_Nx', 
             'M2T1NXINT' : 'tavg1_2d_int_Nx', 
             'M2T1NXLFO' : 'tavg1_2d_lfo_Nx', 
             'M2T1NXLND' : 'tavg1_2d_lnd_Nx', 
             'M2T1NXOCN' : 'tavg1_2d_ocn_Nx', 
             'M2T1NXRAD' : 'tavg1_2d_rad_Nx', 
             'M2T1NXSLV' : 'tavg1_2d_slv_Nx', 
             'M2T3NXGLC' : 'tavg3_2d_glc_Nx', 
             'M2T3NVASM' : 'tavg3_3d_asm_Nv', 
             'M2T3NPCLD' : 'tavg3_3d_cld_Np', 
             'M2T3NVCLD' : 'tavg3_3d_cld_Nv', 
             'M2T3NEMST' : 'tavg3_3d_mst_Ne', 
             'M2T3NPMST' : 'tavg3_3d_mst_Np', 
             'M2T3NVMST' : 'tavg3_3d_mst_Nv', 
             'M2T3NENAV' : 'tavg3_3d_nav_Ne', 
             'M2T3NPODT' : 'tavg3_3d_odt_Np', 
             'M2T3NPQDT' : 'tavg3_3d_qdt_Np', 
             'M2T3NPRAD' : 'tavg3_3d_rad_Np', 
             'M2T3NVRAD' : 'tavg3_3d_rad_Nv', 
             'M2T3NPTDT' : 'tavg3_3d_tdt_Np', 
             'M2T3NETRB' : 'tavg3_3d_trb_Ne', 
             'M2T3NPTRB' : 'tavg3_3d_trb_Np', 
             'M2T3NPUDT' : 'tavg3_3d_udt_Np'} 

TYPE2CODE = SWAPKEYVAL( CODE2TYPE )
TIME2CODE = SWAPKEYVAL( CODE2TIME )
FREQ2CODE = SWAPKEYVAL( CODE2FREQ )
HRES2CODE = SWAPKEYVAL( CODE2HRES )
VRES2CODE = SWAPKEYVAL( CODE2VRES )
GRUP2CODE = SWAPKEYVAL( CODE2GRUP )
COLL2ESDT = SWAPKEYVAL( ESDT2COLL )

def checkval(func):
  @wraps(func)
  def checkvalWrapped(self, *args):
    if len(args) == 0 or args[0] is None: return
    args = list(args)
    for i in range( len(args) ):
      if isinstance(args[i], str):
        args[i] = int(args[i]) if args[i].isdigit() else args[i].upper()
    return func(self, *args)
  return checkvalWrapped

class EarthScienceDataType():
  """
  Earth Science Data Type (ESDT) class for parsing ESDT codes into
  human-readable text. Also enables easy building of data URLs on
  OPeNDAP servers.
  """

  # Attributes defining the data set
  _type     = None      # Data type attribute
  _T        = None      # Data time attribute; Instantaneous, averaged, etc.
  _F        = None      # Data frequency attribute; hourly, monthly, etc
  _H        = None      # Data horizontal resolution attribute
  _V        = None      # Data vertical resolution
  _GGG      = None      # Data group
  _version  = ''        # Data version

  # These attributes store the names of the longitude, latitude, level, and time variables in a given data type
  lonVar    = None
  latVar    = None
  levVar    = None
  timeVar   = None

  # URLS for various datasets
  MERRA2_2D = 'https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap'
  MERRA2_3D = 'https://goldsmr5.gesdisc.eosdis.nasa.gov/opendap'

  def __init__(self, dtype=None, time=None, frequency=None, hres=None, vres=None, group=None, version=None):
    self.setType(      dtype     )
    self.setTime(      time      )
    self.setFrequency( frequency )
    self.setHRes(      hres      )
    self.setVRes(      vres      )
    self.setGroup(     group     )
    self.setVersion(   version   )

  def __iter__(self):
    return (item for item in [self._type, self._T, self._F, self._H, self._V, self._GGG])

  def __str__(self):
    tmp = '{}{}{}{}{}{}'.format(*self)
    return tmp
    if self._version: 
      return '{}.{}'.format(tmp, self._version)
    return tmp

  def __repr__(self):
    return os.linesep.join(
            ['< Earth Scient Data Types (ESDT) >',
             '----------------------------------',
             'Data Type  : ' + CODE2TYPE[self._type],
             'Time Desc. : ' + CODE2TIME[self._T],
             'Frequency  : ' + CODE2FREQ[self._F],
             'Hor. Res.  : ' + CODE2HRES[self._H],
             'Ver. Res.  : ' + CODE2VRES[self._V],
             'Group      : ' + CODE2GRUP[self._GGG],
             'Version    : ' + self._version] )
  @property
  def collection(self):
    return ESDT2COLL.get( str(self), None )

  @property
  def is2D(self):
    return self._V == 'X' 

  @checkval
  def setType(self, val):
    tmp = TYPE2CODE.get(val, None)
    if tmp is None:
      if val not in CODE2TYPE: 
        raise Exception('Unrecognized data collection option: {}'.format(val) ) 
      tmp = val
    self._type   = tmp
    self.lonVar  = VARCOORD['longitude'].get(self._type, None)                  # Set name of longitude variable for this data type
    self.latVar  = VARCOORD['latitude' ].get(self._type, None)
    self.levVar  = VARCOORD['level'    ].get(self._type, None)
    self.timeVar = VARCOORD['time'     ].get(self._type, None)

  @checkval
  def setTime(self, val):
    tmp = TIME2CODE.get(val, None)
    if tmp is None:
      if val not in CODE2TIME:
        raise Exception('Unrecognized time option: {}'.format(val) ) 
      tmp = val
    self._T = tmp

  @checkval
  def setFrequency(self, val):
    tmp = FREQ2CODE.get(val, None)
    if tmp is None:
      if val not in CODE2FREQ:
        raise Exception('Unrecognized frequency option: {}'.format(val) ) 
      tmp = val
    self._F = tmp

  @checkval
  def setHRes(self, val):
    tmp = HRES2CODE.get(val, None)
    if tmp is None:
      if val not in CODE2HRES:
        raise Exception('Unrecognized horizontal resolution option: {}'.format(val) ) 
      tmp = val
    self._H = tmp

  @checkval
  def setVRes(self, val):
    tmp = VRES2CODE.get(val, None)
    if tmp is None:
      if val not in CODE2VRES: 
        raise Exception('Unrecognized vertical resolution option: {}'.format(val) ) 
      tmp = val
    self._V = tmp

  @checkval
  def setGroup(self, val):
    tmp = GRUP2CODE.get(val, None)
    if tmp is None:
      if val not in CODE2GRUP: 
        raise Exception('Unrecognized group option: {}'.format(val) )
      tmp = val
    self._GGG = tmp

  @checkval
  def setVersion(self, *args):
    """
    Set data version to use

    Arguments:
      *args : Major, minor, patch version for data. Can be single string
        in format 'XX.YY.ZZ'

    Keyword arguments:
      None.

    Returns:
      None.

    """

    self._version = '.'.join( [str(arg) for arg in args] )

  def parseString(self, val):
    """
    Parse a MERRA2 data type string; e.g., M2C0NXASM

    Arguments:
      val (str) : Data type string to parse. Can include version number.

    Keywords arguments:
      None.

    Returns:
      None.

    """
 
    if not isinstance(val, str):
      raise Exception('Input must be a string!')
    if len(val) < 9:
      raise Exception( 'ESDT Names must be 9 characters!' )
    self.setType(      val[:2] )
    self.setTime(      val[2]  )
    self.setFrequency( val[3]  )
    self.setHRes(      val[4]  )
    self.setVRes(      val[5]  )
    self.setGroup(     val[6:9] )
    tmp = val.split('.')
    if len(tmp) == 4:
      self.setVersion( *tmp[1:] )

  def _merra2Dates(self, startDate, endDate, endpoint):
    """
    Generator that returns dates within time span

    Based on the ESDT frequency set, dates will be yielded between
    the date range specified

    Arguments:
      startDate (datetime) : Starting date
      endDate   (datetime) : Ending date
      endpoint (bool)      : If True, endDate is included in generator

    Keyword arguments:
      None.
    
    Returns:
      datetime 

    """

    if self._F in (1, 3, 6, 'D'):                                               # For all frequencies less than or equal to one (1) day
      dateInc = lambda x: x + timedelta( days = 1 )                             # Define lambda function to increment date by one (1) day
    else:                                                                       # For all other frequencies
      dateInc = next_month                                                      # Use the monthInc function

    while startDate < endDate:                                                  # With startDate is less than endDate
      yield startDate                                                           # Yield the start date
      startDate = dateInc( startDate )                                          # Increment the startDate using dateInc function
    if endpoint and startDate == endDate:                                       # If the endpoint bool is True AND startDate == endDate
      yield startDate                                                           # Yield startDate

  def getDates(self, startDate, endDate, endpoint=False):
    """
    Create generator that produces valid dates for data files

    This method will return a generator that produces valid dates for
    data files on the remote server between the specified start and end
    dates.

    Arguments:
      startDate (datetime) : Starting date for data period
      endDate (datetime)   : Ending date for data period

    Keyword arguments:
      endpoint (bool) : If set, the endDate will be included in period

    Returns:
      generator

    """

    if self._type == 'M2':                                                      # If MERRA2 data type
      return self._merra2Dates(startDate, endDate, endpoint)
    else:
      raise Exception('Data type not supported : {}'.format(self._type))

  def getFileName(self, date, ext='nc4'):
    """
    Construct a filename based on attributes and date input

    Arguments:
      date (datetime) : Date of file to grab

    Keywords:
      None.

    Returns:
      str : File name

    """

    if self.collection is None:
      raise Exception('Invalid collection, check data exists!')
    if ext[0] == '.':
      ext = ext[1:]
    
    datefmt = '%Y%m'
    if self._F in (1, 3, 6, 'D'):                                               # For all frequencies less than or equal to one (1) day
      datefmt += '%d'

    dstr = date.strftime( datefmt )
    tmp  = [self.collection, dstr, ext]
    if self._type == 'M2':
      tmp = [ getMerraStream(date) ] + tmp 
    return '.'.join( tmp )

  def getStream(self, date):
    """
    Get MERRA2 data stream

    MERRA2 data for certain periods come from different streams; e.g.,
    MERRA2_100, MERRA2_200, etc. Given a datetime object, this method
    returns the correct data stream

    Arguments:
      date (datetime.datetime) : Date of interest to get data stream

    Keyword arguments:
      None.

    Returns:
      str : MERRA2 data stream for given date

    """

    return getMerraStream(date)

  def getDirName(self, date, sep = '/'):
    """
    Get partial directory path to data file on remote server

    Will give the directory path to data on remote server excluding
    the base URL for the data. Thus, this method can be used to generate
    paths for local data downloading

    Arguments:
      date (datetime.datetime) : Date of interest to get data stream

    Keyword arguments:
      sep (str) : Character to use for path separation. For local
        directory, should be set to os.path.sep. Default is '/' for
        use in URLs. 

    Returns:
      str : Directory path 

    """

    tmp = str(self)                                                             # Get string representation of the class
    if self._type == 'M2':                                                      # If type is M2
      if self._version is None:                                                 # If version is None
        raise Exception('Must set version for MERRA2 data!')                    # Raise exception

      tmp  = '{}.{}'.format(tmp, self._version)                                 # Set 
      ddir = 'MERRA2'                                                           # top-level directory base
      if self._F == 'M' or self._F == 0:                                        # If is 'M' or zero
        ddir += '_MONTHLY'                                                      # Append _MONTHLY
      elif self._F == 'U':                                                      # else if U
        ddir += '_DIURNAL'                                                      # Append _DIURNAL

      tmp = [ddir, tmp, date.strftime('%Y')]                                    # Build directory path in list
      if self._F in (1, 3, 6, 'D'):                                             # For all frequencies less than or equal to one (1) day
        tmp.append( date.strftime('%m') )                                       # Append month
    else:
      raise Exception('Data type not yet supported: {}'.format(self._type))     # Raise exception for unsupported data types

    return sep.join( tmp )

  def getPath(self, date, sep = '/'):
    """
    Get partial data file path on remote server

    Will give the path to data on remote server excluding the base URL
    for the data. Thus, this method can be used to generate paths for
    local data downloading

    Arguments:
      date (datetime.datetime) : Date of interest to get data stream

    Keyword arguments:
      sep (str) : Character to use for path separation. For local
        directory, should be set to os.path.sep. Default is '/' for
        use in URLs. 

    Returns:
      str : Data path 

    """

    return sep.join( [self.getDirName(date), self.getFileName(date)] )

  def getBaseURL(self):
    if self._type == 'M2':
      return self.MERRA2_2D if self._V == 'X' else self.MERRA2_3D
    else:
      raise Exception('Data type not yet supported: {}'.format(self._type))

  def getFullURL(self, date):
    """
    Build full URL path to remote file give file date
    """

    base = self.getBaseURL()
    path = self.getPath( date )
    return f'{base}/{path}'
