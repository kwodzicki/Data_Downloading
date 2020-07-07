import logging
import os, json
from datetime import datetime

import numpy as np
from netCDF4 import Dataset, date2num

from ..utils.html_utils import download

from . import getOutDir, URLS

INFO    = URLS['PDO']['info']
URL     = URLS['PDO']['data']
DATEFMT = '%Y%m'
SCALE   = np.float32(1.0e-2)

def getPDO(outRoot, **kwargs):
  """
  Function to download NCEI PDO index values and save as netCDF

  Arugments:
    outRoot (str): Top-level output directory.
      File will be saved in :code:`/outRoot/Teleconnections/NCEI_PDO_Indces.nc`

  Keyword arguments:
    **kwargs: Any arguments accepted by netCDF4.Dataset()

  Returns:
    bool: True on successful download/creation, False otherwise.

  """

  log     = logging.getLogger(__name__)

  if 'format' not in kwargs: kwargs['format'] = 'NETCDF4'											# Set default file format to netCDF4
  if 'mode'   not in kwargs: kwargs['mode']   = 'w'														# Set default mode to write

  outDir  = os.path.join(outRoot, 'Teleconnections')													# Set top-level direction
  outFile = os.path.join(outDir,  'NCEI_PDO_Index.nc')												# Set output file path

  data    = download( URL )																										# Try to download data
  if data is False:																														# If data is False, then failed to download
    log.error('Failed to download data from: {}'.format(URL) )
    return False

  os.makedirs( outDir, exist_ok = True )																			# Make output directories

  data  = json.loads( data )																									# Load JSON data

  dates = []																																	# List for dates
  vals  = []																																	# List for values
  for key, val in data['data'].items():																				# Iterate over key/value pairs (date/value) in the data 
    dates.append( datetime.strptime(key, DATEFMT) )														# Convert key to datetime
    vals.append(  float(val) )																								# Convert value to float

  dates, vals = zip( *sorted( zip(dates, vals), key = lambda x: x[0] ) )			# Ensure data is sorted by date
  vals  = np.asarray( vals )																									# Convert vals to numpy array

  units = 'days since {}'.format(dates[0])																		# Set time units
  num   = date2num( dates, units )																						# Convert dates to numbers

  oid  = Dataset(outFile, **kwargs)																						# Create netCDF file
  oid.description = data['description']['title']															# Set description
  oid.source      = URL 
  oid.information = INFO
  oid.history     = 'Created: {}UTC'.format(datetime.utcnow())
  oid.json2netCDF = __name__

  did = oid.createDimension('time', size=None)																# Define time dimension

  vid = oid.createVariable( 'time', num.dtype, dimensions=('time',))					# Define time variable
  vid.units = units																														# Set units
  vid[:]    = num																															# Write values

  vid = oid.createVariable( 'pdo', 'i2', dimensions=('time',))								# Define pdo variable, scale to 2-byte int to save some space
  vid.units         = 'arbitrary'
  vid.description   = data['description']['title']
  vid.scale_factor  = SCALE																										# Set scaling
  vid.add_offset    = np.float32(0.0)																					# Set offset; no ofset
  vid.missing_value = (data['description']['missing']/SCALE).astype(np.int16)	# Set missing value; must scale the value
  vid[:]            = vals																										# Write values; will apply scaling automatically
  
  oid.close()																																	# Close file

  return True
