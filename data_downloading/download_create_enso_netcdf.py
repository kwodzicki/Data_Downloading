#!/usr/bin/env python3
#+
# Name:
#   download_create_enso_netcdf
# Purpose:
#   A python script to download various ENSO indices and place them all in a 
#   single netCDF file.
#     Indices downloaded include:
#				ESRL/NOAA Nino 3.4					(1948 - present)*
#				ESRL/NOAA Nino 3.4					(1870 - present based on HadiSST)*
#				ESRL/NOAA Nino 4						(1948 - present)*
#				ESRL/NOAA Nino 1+2 					(1948 - present)*
#				ESRL/NOAA Ocean Nino Index 	(1950 - present)*
#				ESRL/NOAA Trans Nino Index	(1948 - present)*
#				ESRL Multivariate ENSO Index (1950 - present)**
#
# Author and History:
#   Kyle R. Wodzicki     Created 14 Jun. 2017
# References:
#   * https://climatedataguide.ucar.edu/climate-data/nino-sst-indices-nino-12-3-34-4-oni-and-tni
#   ** https://www.esrl.noaa.gov/psd/enso/mei/
#-

import logging
import os
from urllib.request import urlopen
import numpy as np
from datetime import datetime
from netCDF4 import Dataset, date2num


url_base  = 'https://www.esrl.noaa.gov/psd/'
dateFMT   = '{:04d}-{:02d}'

class ENSO_Indices( object ):
  nino34_1	= url_base + 'data/correlation/nina34.data'													# ESRL/NOAA, Nino 3.4, ascii text (1948-present)*
  nino34_2	= url_base + 'gcos_wgsp/Timeseries/Data/nino34.long.data'						# ESRL/NOAA Nino 3.4, 1870-present based on HadISST*
  nino4			= url_base + 'data/correlation/nina4.data'													# ESRL/NOAA, Nino 4, ascii text (1948-present)*
  nino12		= url_base + 'data/correlation/nina1.data'													# ESRL/NOAA, Nino 1+2, ascii text (1948-present)*
  oni				= url_base + 'data/correlation/oni.data'														# ESRL/NOAA, Ocean Nino Index, ascii text (1950-present)*
  tni				= url_base + 'data/correlation/tni.data'														# ESRL/NOAA, Trans Nino Index, ascii text (1948-present)*
  mei       = url_base + 'enso/mei/data/meiv2.data'																	# ESRL Multivariate ENSO Index (1950 - present)**
  oneMonth  = np.timedelta64( 1, 'M' )

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.log = logging.getLogger(__name__)

  def allData(self):
    data = {}
    for func in dir(self):
      if func.startswith('get'):
        key, val = getattr( self, func )()
        data[key] = val
    return data

  #==========================================================================
  # Get and parse the ESRL/NOAA, Nino 3.4, ascii text (1948-present)*
  def getNino34(self):
    data = self.download( self.nino34_1 )																		# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data )																					# Parse the data
    out = { 'values'      : vals,
            'time'        : time,
            '_FillValue'	: np.float32(info[0]),
    				'description'	: info[1],
    				'data_url'		: self.nino34_1, 
    				'info_url'		: info[2] }

    out['time_bounds'] = self.timeBounds( time )
    return 'Nino34', out 

  #===============================================================================
  # Get and parse the ESRL/NOAA Nino 3.4, 1870-present based on HadISST*
  def getNino34_HadISST(self):
    data = self.download( self.nino34_2 );																							# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data );																							# Parse the data
    out = { 'values'      : vals,
            'time'        : time,
            '_FillValue'	: np.float32(info[0]),
    				'description'	: info[1] + ' ' + info[3],
    				'domain'			: info[2],
    				'data_url'		: self.nino34_2, 
    				'info_url'		: info[4] }

    out['time_bounds'] = self.timeBounds( time )
    return 'Nino34_HadISST', out 

  #===============================================================================
  # Get and parse the ESRL/NOAA, Nino 4, ascii text (1948-present)*
  def getNino4( self ):
    data = self.download( self.nino4 );																									# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data );																							# Parse the data
    out = { 'values'      : vals,
            'time'        : time,
            '_FillValue'	: np.float32(info[0]),
    				'description'	: info[1],
    				'data_url'		: self.nino4, 
    				'info_url'		: info[2] }

    out['time_bounds'] = self.timeBounds( time )
    return 'Nino4', out 

  #===============================================================================
  # Get and parse the ESRL/NOAA, Nino 1+2, ascii text (1948-present)*
  def getNino12( self ):
    data = self.download( self.nino12 );																								# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data );																							# Parse the data
    out = { 'values'      : vals,
            'time'        : time,
            '_FillValue'	: np.float32(info[0]),
    				'long_name'		: 'Nino1+2 Index',
    				'description'	: info[1],
    				'data_url'		: self.nino12, 
    				'info_url'		: info[2] }

    out['time_bounds'] = self.timeBounds( time )
    return 'Nino12', out

  #===============================================================================
  # Get and parse the ESRL/NOAA, Ocean Nino Index, ascii text (1950-present)*
  def getONI( self ):
    data = self.download( self.oni );																										# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data );																							# Parse the data
    out = { 'values'       : vals,
            'time'        : time,
            '_FillValue'		: np.float32(info[0]),
    				'long_name'		: 'Ocean Nino Index',
    				'description'	: info[1],
    				'data_url'		: self.oni, 
    				'info_url'		: ''.join(info[2:4]),
    				'note'				: ''.join(info[4:-1]) }

    out['time_bounds'] = self.timeBounds( time )
    return 'ONI', out
    
  #===============================================================================
  # Get and parse the ESRL/NOAA, Trans Nino Index, ascii text (1948-present)*
  def getTNI( self ): 
    data = self.download( self.tni )																									# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data )																							# Parse the data
    out = { 'values'      : vals,
            'time'        : time,
            '_FillValue'	: np.float32(info[0]),
    				'long_name'		: 'Trans Nino Index',
    				'description'	: info[2],
    				'data_url'		: self.tni, 
    				'info_url'		: ''.join(info[8:]),
    				'note'				: info[3],
    				'reference'		: ' '.join(info[5:8]) }

    out['time_bounds'] = self.timeBounds( time )
    return 'TMI', out

  #===============================================================================
  # Get and parse the ESRL Multivariate ENSO Index (1950 - present)**
  def getMEI(self):
    data = self.download( self.mei )																						# Get the data from the URL
    vals, time, info = self.parse_ascii_text( data )														# Parse the data
    
    out = { 'values'      : vals,
            'time'        : time,
            '_FillValue'	: np.float32( info[0] ),
				    'long_name'		: 'Multivariate ENSO Index',
    				'description'	: ''.join(info[4:9]),
    				'data_url'		: self.mei, 
    				'info_url'		: os.path.dirname( self.mei ),
    				'note'				: ''.join(info[11:16]) }

    out['time_bounds'] = self.timeBounds( time, MEI=True )
    return 'MEI', out 

  #===============================================================================
  # Method to download data from a url
  def download( self, url ):
    resp = None
    self.log.debug('Trying to open URL: {}'.format(url) )
    try:
      resp = urlopen( url )
    except:
      self.log.error( 'Failed to open URL: {}'.format( url ) )
      return None
    else:
      charset = resp.info().get_content_charset()

    self.log.debug('Trying to get data from URL: {}'.format(url) )
    try:
      data = resp.read()
    except:
      self.log.error( 'Failed to get data from URL: {}'.format(url) )
      data = None

    self.log.debug('Trying to close URL: {}'.format(url) )
    try:
      resp.close()
    except:
      pass

    return data.decode( charset )

  #===============================================================================
  # Method for parsing the ASCII data
  def parse_ascii_text( self, data ):
    data = 'n'.join(data.split('\xc3\xb1'))
    lines = data.split('\n');																											# Split the data on carriage returns
    if lines[-1] == '': lines = lines[:-1];																				# If last line is empty, remove it
    dates, values, extra = [], [], [];
    for i in range(1, len(lines)):
      line = lines[i].split();																										# Split the line
      if len(line) == 0: continue;
      if (len(line[0]) == 4 and line[0].isdigit()): 															# If the first value in the line is a ( four (4) characters long AND only digits) continue
        for j in range(1, 13):																										# Iterate over all the months
          dates.append( dateFMT.format( int(line[0]), j ) )
          if j < len(line): values.append( np.float32(line[j]) );									# Append the data values to the values list after converting them to floats
      else:
        extra.append( lines[i].strip() );																					# Append extra lines to the extra list
    dates = np.array( dates, dtype = 'datetime64' )
    return values, dates, extra


  #===============================================================================
  # Method for computing time bounds, returns (n, m) array 
  def timeBounds( self, time, MEI = False ):
    if MEI:
      return np.asarray( [time - self.oneMonth, time + self.oneMonth] )
    else:
      return np.asarray( [time, time + self.oneMonth] )
    
  #===============================================================================
  # Method for aligning all indices in data in time
  def alignTimes( self, data ):
    oneMonth = np.timedelta64( 1, 'M' )
    dtype    = 'datetime64[M]'
    minTime  = None
    maxTime  = None
    for key in data:
      if not minTime:
        minTime = data[key]['time'].min()
        maxTime = data[key]['time'].max()
      else:
        if (data[key]['time'].min() < minTime):
          minTime = data[key]['time'].min()
        if (data[key]['time'].max() > maxTime):
          maxTime = data[key]['time'].max()

    minTime = np.datetime64( minTime )
    maxTime = np.datetime64( maxTime )
    times   = np.arange( minTime, maxTime + oneMonth, dtype = dtype )

    for key, val in data.items():
      tmp = np.full( times.size, val['_FillValue'] )
      tid = np.where( val['time'][0] == times )[0]
      if (tid.size == 1):
        tmp[tid[0]:tid[0]+val['time'].size] = val['values']
        val['values']      = tmp
        val['time']        = times
        val['time_bounds'] = self.timeBounds( times, MEI=(key == 'MEI') )
    return data

  #===============================================================================
  # Method for converting all times to numbers for netCDF
  def date2num( self, times, units, **kwargs ):
    minTime = datetime.strptime( str( times.min() ), '%Y-%m' )
    try:
      units = units.format( minTime )
    except:
      pass
    if (times.ndim > 1):
      return np.asarray( [ self.date2num( t, units, **kwargs )[0] for t in times ] ), units
    else:
      return date2num([ datetime.strptime(str(t), '%Y-%m') for t in times ], units, **kwargs), units

  #===============================================================================
  # Method for writing data to netCDF file
  def saveData(self, outFile, data ):
    data  = self.alignTimes( data )
    nIDs  = len( data )
    key   = list(data)[0]
    nTime = data[key]['time'].size 
    times, tUnits = self.date2num( data[key]['time'], 'days since {}' )

    self.log.debug( 'Initializing file' )
    oid   = Dataset(outFile, 'w');																										# Open a netCDF file for writing
    oid.description = "This file contains {} different ENSO indices. ".format(nIDs) + \
                      "The indices have been aligned in time."
    oid.history			= "Created by Kyle R. Wodzicki: " + str( datetime.now() );

    tid = oid.createDimension("time",        nTime )														# Create time dimension
    tid = oid.createDimension("time_bounds", 2)    

    vid       = oid.createVariable("time", "f8", ("time",))
    vid.units = tUnits
    vid[:]    = times

 
 
    for key, val in data.items():
      self.log.debug('Creating variable: {}'.format(key) )

      if ('_FillValue' in val):
        fill_value = val.pop('_FillValue')
      else:
        fill_value = None

      vid = oid.createVariable(key, "f4", ("time",), fill_value = fill_value)		# Create variable in netCDF file

      for att in val:																														# Iterate over all tags in the variable dictionary
        if (att != 'values') and ('time' not in att):														# If the tag is not 'values', 'year', or 'month'
          vid.setncattr( att, val[att] )																				# Add the attribute
      vid[:] = val['values']

      timeKey   = '{}_time_bounds'.format(key)
      self.log.debug('Creating time bounds: {}'.format(timeKey))
      vid           = oid.createVariable(timeKey, "f8", ("time_bounds", "time",))
      times, tUnits = self.date2num( val['time_bounds'], 'days since {}' )
      vid.units     = tUnits 
      vid.description = 'Time boundaries for {}'.format(key)
      vid[:] = times

      if fill_value: val['_FillValue'] = fill_value
 
    oid.close()																																		# Close the netCDF file

# Run at command line
if __name__ == "__main__":
  import sys
  if len( sys.argv ) != 2:
    print('Must enter output file path!')
    exit(1)
  handler = logging.StreamHandler()
  handler.setFormatter( logging.Formatter( '%(asctime)s - %(message)s' ) )
  handler.setLevel( logging.DEBUG )
  
  log = logging.getLogger(__name__)
  log.setLevel( logging.DEBUG )
  log.addHandler( handler )

  enso = ENSO_Indices()
  enso.saveData( sys.argv[1], enso.allData() )
  exit(0)
