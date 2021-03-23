#!/usr/bin/env python
"""
A python function to download ERA-I data based on parameters set in an input
dictionary. This function can be called from the command line

Arguments:
  info : A dictionary containing options for the ecmwfapi 
         ECMWFDataServer().retrieve() function. Possible entries are:
    "target"        : specifies a Unix file into which data is to be written after retrieval or manipulation.
    "source"        : specifies a Unix file from which data is to be read.
    "fieldset"      : args.fieldset});
    "date"          : args.date});
    "time"          : args.time});
    "step"          : args.step});
    "refdate"       : args.refdate});
    "hdate"         : args.hdate});
    "anoffset"      : args.anoffset});
    "reference"     : args.reference});
    "range"         : args.range});
    "fcmonth"       : args.fcmonth});
    "fcperiod"      : args.fcperiod});
    "expect"        : args.expect});
    "padding"       : args.padding});
    "database"      : args.database});
    "dataset"       : args.dataset});
    "use"           : args.use});
    "class"         : args.classif});
    "stream"        : args.stream});
    "expver"        : args.expver});
    "repres"        : args.repres});
    "domain"        : args.domain});
    "origin"        : args.origin});
    "system"        : args.system});
    "method"        : args.method});
    "type"          : args.type});
    "levtype"       : args.levtype});
    "levelist"      : args.levelist});
    "number"        : args.number});
    "param"         : args.param});
    "channel"       : args.channel});
    "diagnostic"    : args.diagnostic});
    "iteration"     : args.iteration});
    "frequency"     : args.frequency});
    "direction"     : args.direction});
    "product"       : args.product});
    "section"       : args.section});
    "latitude"      : args.latitude});
    "longitude"     : args.longitude});
    "grid"          : args.grid});
    "area"          : args.area});
    "frame"         : args.frame});
    "resol"         : args.resol});
    "rotation"      : args.rotation});
    "interpolation" : args.interpolation});
    "accuracy"      : args.accuracy});
    "packing"       : args.packing});
    "specification" : args.specification});
    "style"         : args.style});
    "bitmap"        : args.bitmap});
    "format"        : 'netcdf'});
Outputs:
  Downloads a file and returns a code (see Return Codes) indicating completion
  of script
Keywords:
  verbose : Set to True to increase verbosity
  timeout : Set the timeout for a download, in minutes. Default is 4 hours.
Return codes
  0 : Download completed normally
  1 : Uncompressed file already existed
  2 : Compressed file already exists; must have convention 
      'NameOfFile_gzip.ext'
  3 : File failed to download after three attempts
  4 : Date NOT set
Author and History:
  Kyle R. Wodzicki     Created 12 Jun. 2017
Notes
  Standard atmospheric levels:
    1000/925/850/700/500/400/300/250/200/150/100
"""

import logging;
from logging.handlers import RotatingFileHandler

import os, sys, time, json;
import math
from datetime import datetime, timedelta

from multiprocessing import Process, Queue;
from threading import Thread
from calendar import monthrange;
from datetime import date;
from ecmwfapi.api import ECMWFDataServer, APIRequest;                           # Import the ECMWF library

from data_downloading import ERAI_LOGDIR
from data_downloading.utils.dateutils import next_month

STARTDATE = datetime(1979, 1, 1, 0)
ENDDATE   = datetime(2019, 8, 1, 0)

def multiProcDownload( request, **kwargs ):
  inst = ECMWF( **kwargs )
  return inst.retrieve( request )
  
################################################################################
class ECMWF( ECMWFDataServer ):
  def __init__(self, *args, **kwargs):
    quiet = kwargs.pop('quiet', False)
    super().__init__(*args, **kwargs)
    self.quiet = quiet
    self.__log = logging.getLogger(__name__)
  ##############################################################################
  def retrieve(self, req):
    '''
    Overload the retrieve method so that can add the 'quiet' keyword to 
    APIRequest call
    '''
    target  = req.get("target")
    dataset = req.get("dataset")
    c = APIRequest(self.url, "datasets/%s" % (dataset,), 
          email   = self.email, 
          key     = self.key, 
          log     = self.trace, 
          quiet   = self.quiet,
          verbose = self.verbose)
    return c.execute(req, target);

################################################################################
class ERAI_Downloader( object ):
  """Class for downloading ERA-Interim data"""

  def __init__(self, logfile = None, verbose = False, timeout = 240, netcdf = False, **kwargs):
    super().__init__();
    self.log     = logging.getLogger(__name__)
    self.log.setLevel( logging.INFO )
    self.logfile = logfile;                                                     # Set the logfile path
    self.verbose = verbose;                                                     # Set verbosity
    self.timeout = timeout;                                                     # Set default timeout
    self.netcdf  = netcdf;                                                      # Set netcdf
    self.status  = 0;                                                           # Set status of download to zero (0)

    self.info = {
      "class"    : "ei",
      "dataset"  : "interim",
      "expver"   : "1",
      "grid"     : "1.5/1.5",
      "area"     : "90/0/-90/360",
      "stream"   : "oper",
      "levtype"  : "pl",
      "type"     : "an",
      "time"     : "00:00:00",
      "step"     : "0",
      "levelist" : '',
      "param"    : '',
      "target"   : ''
    }
    self.info.update( kwargs )

    if logfile is not None:
      self.logfile = logfile
    else:
      self.logfile = self.defaultLogfile()

    if not os.path.isdir( os.path.dirname(self.logfile) ):
      os.makedirs( os.path.dirname( self.logfile) )

    rfh = RotatingFileHandler( self.logfile, 
      maxBytes = 1024 * 10**3, backupCount = 1
    )
    rfh.setLevel( logging.DEBUG )
    self.log.addHandler( rfh )

    self.startDate = None
    self.endDate   = None
 
    self._queue = Queue()
    self._thread = Thread( target = self._receive, daemon = True )  
    self._thread.start()

  ##############################################################################
  def file_test(self):
    """
    Check that the downloaded file is larger than the minimum file size

    """
    exist = False;                                                              # Set exist to False
    if os.path.isfile( self.info['target'] ):                                   # If the path is a file
      fileinfo = os.stat( self.info['target'] );                                # Get information about the file
      if fileinfo.st_size > self.est_size:                                      # If the file size is greater than the estimated size
        exist = True;                                                           # Set exits to True
    return exist;                                                               # Return exist

  def _parseStartDate(self):
    """
    Parse a date string
    """
    date = self.info['date'].split('/')[0]
    try:
      return datetime.strptime(date, '%Y-%m-%d')
    except:
      pass
    try:
      return datetime.strptime(date, '%Y-%m')
    except:
      raise Exception('Failed to parse starting date') 
   
  ##############################################################################
  def defaultTarget(self):
    if self.startDate and self.info.get('type','') and self.info.get('levtype',''):
      date     = self._parseStartDate()
      baseName = date.strftime('%Y%m%dT%H%M%SZ.nc')
      dirs     = [ self.info['grid'].replace('/', 'x') ]
      if self.info['levtype'] == 'ml':
        dirs.append( 'model' )
      elif self.info['levtype'] == 'pl':
        dirs.append( 'pressure')
      elif self.info['levtype'] == 'sfc':
        dirs.append( 'single' )
      elif self.info['levtype'] == 'pv':
        dirs.append( 'potential_vorticity')
      elif self.info['levtype'] == 'pt':
        dirs.append( 'isentropic' )
      elif self.info['levtype'] == 'dp':
        dirs.append( 'depth' )
      else:
        raise Exception( 'Unrecognized levtype: {}'.format(self.info['levtype']) )
      dirs.append('netcdf')
      dirs.extend( [date.strftime('%Y'), date.strftime('%Y%m')] )
      return os.path.join( *dirs, baseName )
    return ''

  ##############################################################################
  def defaultLogfile(self):
    if self.info.get('type','') and self.info.get('lvltype',''):
      log = 'ERAI_Download_{}_{}.log'.format(self.info['type'], self.info['levtype'])
    else:
      log = '{}.log'.format(self.__class__.__name__) 
    return os.path.join( ERAI_LOGDIR, 'logs', log )

  ##############################################################################
  def loadJSON( self, jsonFile ):
    """ 
    Method to load variable and level data from a JSON file

    Arguments:
      jsonFile  : Full path to JSON file containing variable and level info

    Keyword arguments:
      None.

    Returns:
      Nones

    """

    if not os.path.isfile( jsonFile ):                                          # If the jsonFile path is NOT a file
      self.log.error('No such file: {}'.format( jsonFile ) );                   # Log error
      return False;                                                             # Return False
    else:                                                                       # Else
      try:                                                                      # Try to...
        with open( jsonFile, 'r' ) as fid:                                      # Open file
          data = json.load( fid );                                              # Parse JSON data
      except:                                                                   # On exception
        self.log.exception('Error loading JSON file');                          # Log the exception
        return False;                                                           # Return False
      else:                                                                     # Else, read was success
        self.info["param"] = '/'.join( map( str, data['variables'] ) );	        # Set params 
        if len(data['levels']) > 0:                                             # If levels in the JSON file
          self.info["levelist"] = '/'.join( map( str, data['levels'] ) );       # Set levels
    return True;                                                                # Return True

  ##############################################################################
  def set_date( self, *args ):
#sYear = None, sMonth = None, sDay = None,
#                      eYear = None, eMonth = None, eDay = None ):
    """
    Set the date for downloading.

    Arguments:
      sdate (datetime): The starting datetime of the download.
      edate (datetime): The ending datetime of the download.


    """

    if len(args) >= 2:
      self.startDate, self.endDate = args[:2]
    else:
      self.startDate = args[0] if len(args) == 1 else STARTDATE
      self.endDate   = next_month(self.startDate) - timedelta(days=1)

    totSeconds   = (self.endDate - self.startDate).total_seconds()
    self.totDays = math.ceil( totSeconds / 86400 ) + 1
    self.info['date'] = "{}/to/{}".format(self.startDate.strftime('%Y-%m-%d'),
                                          self.endDate.strftime(  '%Y-%m-%d') )

    
#    if self.endDate:
#      endDate =   
#    if len(self.startDate) == 2: self.startDate.append('01');                   # If no day in the start date, then append 01
#    if len(self.endDate) == 2:                                                  # Else if the end date is specified but with no day
#      nDays = (monthrange(int(self.endDate[0]), int(self.endDate[1])))[1];      # Get number of days in end date
#      self.endDate.append( '{:02d}'.format(nDays) );                            # Append number of days to self.endDate
#    elif len(self.endDate) == 3:                                                # Else, check that the end day is within the number of days in the month
#      nDays = (monthrange(int(self.endDate[0]), int(self.endDate[1])))[1];      # Get number of days in end date
#      if int(self.endDate[2]) > nDays: self.endDate[2] = '{:02d}'.format(nDays);# If the input self.endDate has more days in the end month than possible, update the end day
#    if len(self.startDate) == 3 and len(self.endDate) == 3:                     # If date ranges input
#      self.info['date'] = '/to/'.join(['-'.join(self.startDate),'-'.join(self.endDate)]);   # Set the new date
#      sDate = date( int(self.startDate[0]), int(self.startDate[1]), int(self.startDate[2]));# Set starting date
#      eDate = date( int(self.endDate[0]),   int(self.endDate[1]),   int(self.endDate[2]));  # Set ending date
#      self.totDays = (eDate - sDate).days + 1;                                  # Total number of days between the starting and ending dates
#    else:
#      self.totDays = 1;

  ##############################################################################
  def estimate_size(self):
    """
    Estimated file size. The variable 'est_size' will be added to the class."""

    # Determine some information about the dimensionality of the downloaded files
    check_info, test = ['area', 'grid', 'time', 'param'], 0;                    # Initialize check for variables required for estimated size check
    for i in check_info:
      if i in self.info: test += 1;
    if test == len(check_info):
#       domain = map(int,   self.info['area'].split('/'));                      # Split the domain on forward slash and map to integers
#       grid   = map(float, self.info['grid'].split('/'));                      # Split the grid on forward slash and map to floats
      domain = [ int(i) for i in self.info['area'].split('/') ];                # Split the domain on forward slash and map to integers
      grid   = [ float(i) for i in self.info['grid'].split('/') ];              # Split the grid on forward slash and map to floats
      xdim   = (domain[3] - domain[1]) / grid[0];                               # Compute number of grid boxes in x (longitude)
      ydim   = (domain[0] - domain[2]) / grid[1] + 1;                           # Compute number of grid boxes in y (latitude)
      ntimes = len(self.info['time'].split('/'));                               # Split the time list on forward slash and get the length of the list; i.e., get the number of time steps requested
      nvars  = len(self.info['param'].split('/'));                              # Split the variable list on forward slash
      if self.info['levtype'] == 'sfc':
        nlvls = 1
      else:
        nlvls = len(self.info['levelist'].split('/'));                          # Split the level list on forward slash and get the length of the list; i.e., get number of levels
      self.est_size = 2 * xdim * ydim * nlvls * ntimes * nvars * self.totDays;  # Set estimated size of the downloaded file
    else:
      self.est_size = 0;                                                        # Set estimated size of the downloaded file to -1; i.e., not computed

  ##############################################################################
  def check_target(self):
    '''Check that there is a target file for the download. If there is none,
    one will be created in the user's home directory with a naming convention
    of info['type']_info['levtype']_sYearsMonth-eYeareMonth. See the set_date
    function for information on sYear, sMonth, eYear, and eMonth.'''
    if 'target' not in self.info:                                               # If the target file is specified
      file = os.path.expanduser("~") + '/' + self.info['type'] + \
              '_' + self.info['levtype'] + '_' + ''.join(self.startDate);       # Set default target file
      if self.endDate != '': file = file + '-' + ''.join(self.endDate);         # Append end date
      file = file + '.nc' if self.netcdf is True else file + '.grib';           # Set file extension
      self.info.update({"target" : file});                                      # Update the parsed dictionary

  ##############################################################################
  def download(self, max_attempt = None):
    '''Function to actually download the data. After the all information is
    set in the info dictionary, run this function to download the data. There
    is an optional input: max_attempt. This sets the maximum number of times
    to attempt to download the file. The default is 3.'''

    if 'date' not in self.info:
      self.log.error( 'No date information!' )
      self.status = 4
      return																																		# Exit the function

    self.max_attempt = max_attempt if max_attempt is not None else 3;           # Set default value of max_attempt to three (3)

    self.check_target();                                                        # Check the file target
    self.estimate_size();                                                       # Estimate size of the downloaded file

    self.comp_file = self.info['target'].split('.');                            # Split target file on period (.)
    self.comp_file = '.'.join(self.comp_file[:-1])+'_gzip.'+self.comp_file[-1]; # Compressed file path

    if self.netcdf: self.info['format'] = 'netcdf'
    for key in list( self.info.keys() ):
      if self.info[key] == '':
        self.info.pop(key)

    if self.file_test():                                                        # If file already exists
      self.status = 1;                                                          # Set status to one (1)
    elif os.path.isfile( self.comp_file ):                                      # If compressed file exists
      self.status = 2;                                                          # Set status to two (2)
    else:
      self._download()

  def _download(self):
    dir = os.path.dirname( self.info['target'] );                             # Initialize output directory
    if not os.path.isdir( dir ): os.makedirs( dir );                          # If directory does NOT exist, then make it
    self.attempt = 0;                                                         # Set download attempt
    while self.attempt < self.max_attempt:                                    # Try max_attempt times to download the file
      try:
        self.elapsed = 0;                                                     # Set elapsed time for download to zero, current time, and timeout to 3 hours (180 minutes)
        self.pid = Process(
          target = multiProcDownload,
          args   = (self.info,),
          kwargs = {'log' : self._queue.put_nowait, 'quiet' : True}
        );                                                                    # Set up background process for downloading
        self.pid.start();                                                     # Start the process
        self._procWait()
      except (KeyboardInterrupt, SystemExit):
        self.log.critical('Interupt or exit encountered!')
        return
      except:                                                                 # On exception
        if os.path.exists( self.info['target'] ): 
          os.remove( self.info['target'] );                                   # If there was an issue with the download, delete the file
        self.attempt += 1;                                                    # If the download failed, increment attempt number
        self.log.warning( 'File: {} Unsuccessful!'.format(self.info['target']) ); # Verbose output
        self.log.warning( 'Restarting download...' )                          # Verbose output
      else:
        if self.file_test():                                                  # If no error thrown, check if file exists and is roughly the correct size
          self.attempt = self.max_attempt+1;                                  # If the download successful, set attempt to one (1) greater than the maximum number of attempts
          self.log.info( 'File: {} Successful!'.format(self.info['target']) );# Verbose output
        else:                                                                 # If the file is not the correct size
          if os.path.exists( self.info['target'] ): 
            os.remove( self.info['target'] );                                 # If the files exists, delete it
          self.attempt += 1;                                                  # If the download failed, increment attempt number
          self.log.warning( 'File: {} Unsuccessful!'.format(self.info['target']) );# Verbose output
          self.log.warning( 'Restarting download...' );                       # Verbose output
    self.status = 0 if self.attempt == self.max_attempt+1 else 3;             # Set the status

  def _procWait(self):
    while self.pid.is_alive():                                            # While the process is alive, i.e., running
      if self.elapsed >= self.timeout:                                    # If the elapsed time exceeds the timeout
        self.pid.terminate();                                             # Kill the download
        raise Exception();                                                # Raise an exception
      else:                                                               # Else
        time.sleep(60);                                                   # Sleep for 60 seconds
      self.elapsed += 1;                                                  # Compute new elapsed time
    if self.verbose:                                                      # If verbose is set
      if (self.elapsed % 10) == 0:                                        # If the elapsed time is a multiple of 10
        self.log.debug( 'Sleep time: {:3d}'.format(self.elapsed) );       # Print out the elapsed time            
    self.pid.join();                                                      # Exit the process cleanly
    self.status = self.pid.exitcode;                                      # Get the exit code from the process

  ##############################################################################
  def _receive(self):
    '''Method to monitor for logs from ECMWFDataServer retrieve'''
    while True:
      try:
        record = self._queue.get();
        self.log.debug( record )
      except (KeyboardInterrupt, SystemExit):
        break
      except:
        pass

################################################################################
if __name__ == "__main__":
  import argparse;                                                              # Import library for parsing
  parser = argparse.ArgumentParser(description="Download ERA-Interim");         # Set the description of the script to be printed in the help doc, i.e., ./script -h
  ### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
  parser.add_argument("--target",       type=str, help="specifies a Unix file into which data is to be written after retrieval or manipulation.")
  parser.add_argument("--source",       type=str, help="specifies a Unix file from which data is to be read."); 
  parser.add_argument("--fieldset",     type=str, help="is a MARS variable into which retrieved data is to be written after manipulation"); 
  ### Date and time keywords; https://software.ecmwf.int/wiki/display/UDOC/Date+and+time+keywords
  parser.add_argument("--date",         type=str, help="Specifies the Analysis date, the Forecast base date or Observations date.")
  parser.add_argument("--time",         type=str, help="Specifies the time of the data in hours and minutes."); 
  parser.add_argument("--step",         type=str, help="Set step size for surface forecast fields."); 
  parser.add_argument("--refdate",      type=str, help="Set step size for surface forecast fields."); 
  parser.add_argument("--hdate",        type=str, help="Specifies the hindcast base date in the VarEPS system."); 
  parser.add_argument("--anoffset",     type=str, help="Specifies the time difference in hours between an analysis time and the end of the assimilation window used to produce that analysis.")
  parser.add_argument("--reference",    type=str, help="Specifies the reference forecast time step (in hours) at which the assignment of Ensemble forecast members to Tube products is carried out.")
  parser.add_argument("--range",        type=str, help="For observations, it denotes the period, in minutes, starting from time. It can be used to specify periods of time over different dates. (i.e., time=2100, range=360, denotes observations from 21:00 to 03:00 next day).")
  parser.add_argument("--fcmonth",      type=str, help="For certain datasets like the seasonal forecast monthly means, it specifies the complete calendar month which follows the forecast base date.")
  parser.add_argument("--fcperiod",     type=str, help="For certain datasets like the monthly forecast monthly means, it defines the period over which the field has been averaged. Its units are dates.")
  ### Execution control keywords; https://software.ecmwf.int/wiki/display/UDOC/Execution+control+keywords
  parser.add_argument("--expect",       type=str, help="specifies the number of fields the request is expected to retrieve.")
  parser.add_argument("--padding",      type=str, help="specifies a number of bytes to use for padding.")
  parser.add_argument("--database",     type=str, help="specifies in which of the available databases the search for the required data should be carried out.")
  parser.add_argument("--dataset",      type=str, help="In the Dataset service there is an extra mandatory keyword called dataset which does not appear in normal MARS requests.")
  parser.add_argument("--use",          type=str, help="provides MARS with a hint about the use of the retrieved data.")
  ### Identification keywords; https://software.ecmwf.int/wiki/display/UDOC/Identification+keywords
  parser.add_argument("--classif",      type=str, help="specifies the ECMWF classification given to the data.")
  parser.add_argument("--stream",       type=str, help="identifies the forecasting system used to generated the data when the same meteorological types are archived."); 
  parser.add_argument("--expver",       type=str, help="is the version of the data.")
  parser.add_argument("--repres",       type=str, help="selects the representation of the archived data (sh for spherical harmonics, gg for Gaussian grid or ll for latitude/longitude).")
  parser.add_argument("--domain",       type=str, help="Specifies a geographical domain for which the data has been produced.");
  parser.add_argument("--origin",       type=str, help="Specifies the origin of the data, usually as the CCCC WMO centre identifier.");
  parser.add_argument("--system",       type=str, help="Labels the version of the operational forecast system for seasonal forecast related products.");
  parser.add_argument("--method",       type=str, help="Used for seasonal forecast related products.");
  ### Identification keywords specific for fields
  parser.add_argument("--type",         type=str, help="determines the type of fields to be retrieved."); 
  parser.add_argument("--levtype",      type=str, help="denotes type of level. Its value has a direct implication on valid levelist values. Common values are: model level (ml), pressure level (pl), surface (sfc), potential vorticity (pv), potential temperature (pt) and depth (dp). Note ocean data is archived with levtype=dp, and wave data is archived with levtype=sfc."); 
  parser.add_argument("--levelist",     type=str, help="specifies the required levels. Valid values have to correspond to the selected levtype. For example, model levels can range from 1 to 91. Pressure levels are specified in hPa, e.g. 1000 or 500. Potential vorticity levels are specified in units of 10-9 m2 s-1 K kg-1. For ocean fields, values are specified in metres below sea level, e.g. 5."); 
  parser.add_argument("--number",       type=str, help="selects the member in ensemble forecast run. It has a different meaning depending on the type of data.")
  parser.add_argument("--param",        type=str, help="specifies the meteorological parameter. A list of variables can be found at http://apps.ecmwf.int/codes/grib/param-db"); 
  parser.add_argument("--channel",      type=str, help="represents the frequency band of the data. This attribute is used for very specific data: brightness temperatures (param=194) from Errors in First Guess data type (type=ef).")
  parser.add_argument("--diagnostic",   type=str, help="specifies the diagnostic function number code in sensitivity forecast products");
  parser.add_argument("--iteration",    type=str, help="specifies the step number in the algorithm for the minimisation of the diagnostic function for sensitivity forecast products. Usual iteration values range from 0 to 3.");
  parser.add_argument("--frequency",    type=str, help="specifies the required frequency components of wave model spectral fields.");
  parser.add_argument("--direction",    type=str, help="specifies the required direction components of wave model spectral fields.")
  parser.add_argument("--product",      type=str, help="For ocean fields, it denotes how the archived field is defined in time.")
  parser.add_argument("--section",      type=str, help="For ocean fields, denotes the spatial orientation of the archived field.")
  parser.add_argument("--latitude",     type=str, help="For ocean fields, on a zonally oriented or time series product, denotes the location in latitude.")
  parser.add_argument("--longitude",    type=str, help="For ocean fields, on a meridionally oriented or time-series product, denotes the location in longitude.")
  ### Identification keywords specific for observations and satellite images in BUFR
  ### Identification keywords specific for ODB
  ### Post-processing keywords; https://software.ecmwf.int/wiki/display/UDOC/Post-processing+keywords
  parser.add_argument("--grid",         type=str, help="specifies the output grid which can be either a Gaussian grid or a Latitude/Longitude grid. MARS requests specifying grid=av will return the archived model grid."); 
  parser.add_argument("--area",         type=str, help="specifies the desired sub-area of data to be extracted."); 
  parser.add_argument("--frame",        type=str, help="specifies the number of points to be selected from a sub-area inwards.")
  parser.add_argument("--resol",        type=str, help="specifies the desired triangular truncation of retrieved data, before carrying out any other selected post-processing.")
  parser.add_argument("--rotation",     type=str, help="specifies a rotation for the output fields.");
  parser.add_argument("--interpolation",type=str, help="can be used to force the interpolation method to bilinear or nearest neighbour for interpolations from grids.");
  parser.add_argument("--accuracy",     type=str, help="specifies the number of bits per value to be used in the generated GRIB coded fields.")
  parser.add_argument("--packing",      type=str, help="specifies the packing method of the output fields.")
  parser.add_argument("--specification",type=str, help="forces an old grid definition to be used (Research Department designated _12).")
  parser.add_argument("--style",        type=str, help="specifies the style of post-processing.")
  parser.add_argument("--bitmap",       type=str, help="specifies a UNIX filename containing a bitmap definition in the format defined in appendix 1 of the  Dissemination manual.")
  ### Extra Options
  parser.add_argument("--logfile", type=str, help="Full path to log file. Default is to place log file in download directory.");
  parser.add_argument("-v",  "--verbose", action="store_true", help="Set for verbose output");
  parser.add_argument("-t",  "--timeout", type=int, nargs='?', const=180, help="Set timeout for download in mintues. Default is 180 minutes.");
  parser.add_argument("-nc", "--netcdf",  action="store_true", help="Set for netCDF output");

  # Parse the options and set up dictionary
  args, info = parser.parse_args(), {};                                            # Parse the arguments and initialize dictionary
  if args.target        is not None: info.update({"target"        : args.target});
  if args.source        is not None: info.update({"source"        : args.source});
  if args.fieldset      is not None: info.update({"fieldset"      : args.fieldset});
  if args.date          is not None: info.update({"date"          : args.date});
  if args.time          is not None: info.update({"time"          : args.time});
  if args.step          is not None: info.update({"step"          : args.step});
  if args.refdate       is not None: info.update({"refdate"       : args.refdate});
  if args.hdate         is not None: info.update({"hdate"         : args.hdate});
  if args.anoffset      is not None: info.update({"anoffset"      : args.anoffset});
  if args.reference     is not None: info.update({"reference"     : args.reference});
  if args.range         is not None: info.update({"range"         : args.range});
  if args.fcmonth       is not None: info.update({"fcmonth"       : args.fcmonth});
  if args.fcperiod      is not None: info.update({"fcperiod"      : args.fcperiod});
  if args.expect        is not None: info.update({"expect"        : args.expect});
  if args.padding       is not None: info.update({"padding"       : args.padding});
  if args.database      is not None: info.update({"database"      : args.database});
  if args.dataset       is not None: info.update({"dataset"       : args.dataset});
  if args.use           is not None: info.update({"use"           : args.use});
  if args.classif       is not None: info.update({"class"         : args.classif});
  if args.stream        is not None: info.update({"stream"        : args.stream});
  if args.expver        is not None: info.update({"expver"        : args.expver});
  if args.repres        is not None: info.update({"repres"        : args.repres});
  if args.domain        is not None: info.update({"domain"        : args.domain});
  if args.origin        is not None: info.update({"origin"        : args.origin});
  if args.system        is not None: info.update({"system"        : args.system});
  if args.method        is not None: info.update({"method"        : args.method});
  if args.type          is not None: info.update({"type"          : args.type});
  if args.levtype       is not None: info.update({"levtype"       : args.levtype});
  if args.levelist      is not None: info.update({"levelist"      : args.levelist});
  if args.number        is not None: info.update({"number"        : args.number});
  if args.param         is not None: info.update({"param"         : args.param});
  if args.channel       is not None: info.update({"channel"       : args.channel});
  if args.diagnostic    is not None: info.update({"diagnostic"    : args.diagnostic});
  if args.iteration     is not None: info.update({"iteration"     : args.iteration});
  if args.frequency     is not None: info.update({"frequency"     : args.frequency});
  if args.direction     is not None: info.update({"direction"     : args.direction});
  if args.product       is not None: info.update({"product"       : args.product});
  if args.section       is not None: info.update({"section"       : args.section});
  if args.latitude      is not None: info.update({"latitude"      : args.latitude});
  if args.longitude     is not None: info.update({"longitude"     : args.longitude});
  if args.grid          is not None: info.update({"grid"          : args.grid});
  if args.area          is not None: info.update({"area"          : args.area});
  if args.frame         is not None: info.update({"frame"         : args.frame});
  if args.resol         is not None: info.update({"resol"         : args.resol});
  if args.rotation      is not None: info.update({"rotation"      : args.rotation});
  if args.interpolation is not None: info.update({"interpolation" : args.interpolation});
  if args.accuracy      is not None: info.update({"accuracy"      : args.accuracy});
  if args.packing       is not None: info.update({"packing"       : args.packing});
  if args.specification is not None: info.update({"specification" : args.specification});
  if args.style         is not None: info.update({"style"         : args.style});
  if args.bitmap        is not None: info.update({"bitmap"        : args.bitmap});
  if args.netcdf        is True:     info.update({"format"        : 'netcdf'});

  dei = download_era_interim( info, 
    logfile = args.logfile, 
    verbose = args.verbose, 
    timeout = args.timeout,
    netcdf  = args.netcdf );
  dei.download();
  
  exit( dei.status );
