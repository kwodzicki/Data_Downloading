#!/usr/bin/env python3
"""
To download variables from MERAA-2 required for ITCZ identification and
mid-level dry-layer identification
These variables include:
  u-wind: 1000-850 hPa
  v-wind: 1000-850 hPa
  temp:   850 hPa
  rh:     850 hPa
  rh:     400-600hPa
  precip: surface.

For RH data:
https://goldsmr5.gesdisc.eosdis.nasa.gov/opendap/hyrax/MERRA2/M2I3NPASM.5.12.4/1980/01/MERRA2_100.inst3_3d_asm_Np.19800101.nc4
RH, U, V, T

For Precip (hourly):
https://goldsmr4.sci.gsfc.nasa.gov/opendap/hyrax/MERRA2/M2T1NXFLX.5.12.4/1980/01/MERRA2_100.tavg1_2d_flx_Nx.19800101.nc4
PRECTOT
"""
import logging

import os
import time

import numpy as np

from ../utils import pydapData

from .utils.interpLonLat import InterpLonLat


def download( esdt, variables, startDate, endDate, outdir, 
        endpoint=False, prefix='', postfix='', callback=None, **kwargs ):
  """
  Download data to given directory over given timespan

  Given and EarthScienceDataType object, this function will download
  all data over the time span [startDate, endDate] to the specified
  directory. The specified directory should be the top level directory
  as this function will maintain the directory structure from the remote
  server. The base file name can be augmented slightly using the prefix
  keyword

  Arguments:
    esdt (EarthScienceDataType) : ESDT object containg information about
      dataset to download.
    variables (list) : List of MERRA2Variable instances to download.
    startDate (datetime) : First date to download; inclusive
    endDate   (datetime) : Last date to download; inclusivity set by endpoint keyword
    outdir (str) : Top-level directory to store data in; remote directory
      structure is preserved.

  Keyword arguments:
    endpoint (bool) : Controls whether the endDate is inclusive or exclusive
    prefix   (str)  : Custom prefix to add to downloaded data files
    postfix  (str)  : Custom postfix to add to downloaded data files
    callback (func) : Function to run after downloading completes.
      This function will be passed a list of all downloaded file paths
    **kwargs : Any extra arguments are passed directly to netCDF4.Dataset

  Returns:
    bool : True if downloads finished successfully, False otherwise

  """

  log = logging.getLogger(__name__)
  files = []
  for date in esdt.getDates( startDate, endDate, endpoint=endpoint ):
    log.info( 'Getting data for : {}'.format( date ) )
    remoteDir  = esdt.getDirName(date)                                             # Get path to data file on remote server
    remoteFile = esdt.getFileName(date)                                         # Get remote file name

    localDir   = os.path.join( outdir, *remoteDir.split('/') )                   # Build path to local file; split the URL path so that os can join properly for whatever system code is run on
    os.makedirs( localDir, exist_ok=True )                                      # Create directory if not exist
    fname, ext = os.path.splitext( remoteFile )
    localFile  = prefix + fname + postfix + ext
    localPath  = os.path.join( localDir, localFile )

    if not downloader( esdt, date, variables, localPath, **kwargs ):
      raise Exception( "Downloading failed" )
    files.append( localPath)

  if callback: callback( files )

  return


def downloader( esdt, date, variables, localfile, dLon=None, dLat=None, **kwargs ):
  """
  Download data from URL

  This function actually does downloading

  Arguments:
    URL (str) : URL of remote date file
    variables (dict) : Dictionary with variables, and levels, to download.
      Keys are the variable names while the values under a key are the
      levels to download. To download specific levels, use scalars, list,
      or tuple data. To download all levels in a give range, use a slice()
      object to define the range of levels. For slices, ensure values are
      ascending; i.e., start > stop values. So, to get all data between
      1000 hPa and 850 hPa, use slice(850, 1000).
    localfile (str) : Full path of local file to download data to

  Keyword arguments:
    dLon (float) : If set, will interpolate data to given longitude
      resolution before writing to file
    dLat (float) : If set, will interpolate data to given latitude
      resolution before writing to file

    **kwargs : Any arguments accepted by netCDF4.Dataset

  Returns:
    bool : True if downloads finished successfully, False otherwise

  """

  from netCDF4 import Dataset, num2date                                         # Import here to ensure works correctly in multiprocessing

  log = logging.getLogger(__name__)                                             # Get logger
 
  
  URL    = esdt.getFullURL( date )
  if os.path.isfile( localfile ):
    log.info('Local file exists, skipping download : {}'.format(localfile) )
    return True

  log.info('Local file  : {}'.format(localfile))
  log.info('Remote file : {}'.format(URL  ))
  
  remote = pydapData.PyDAPDataset( URL, **kwargs )                                        # Open remote file

  if remote is None:
    raise Exception( f'Failed to open remote file : {URL}' )

  lon, _  = remote.getVar( esdt.lonVar )                       # Use the downloadVariable function for exception handling
  lat, _  = remote.getVar( esdt.latVar )
  if not esdt.is2D: lev, _ = remote.getVar( esdt.levVar )
  time, _ = remote.getVar( esdt.timeVar )

  interp = InterpLonLat( lon, lat )
  interp.setLonLatRes( dLon, dLat )
 
  status = True
  log.info( 'Initializing new local file...' );
  local = Dataset(localfile, 'w', **kwargs)
  for var in variables:                                                         # Iterate over variables
    log.info('Working on variable: {}'.format(var.varname) )                    # Log
    if esdt.is2D:                                                               # If 2D data
      slices = var.get2DSlices(lonData=lon, latData=lat, timeData=time)         # Get slices for 2D data
    else:                                                                       # Else
      slices = var.get3DSlices(lonData=lon, latData=lat, levData=lev, timeData=time)    # Slices for 3d

    values, atts = remote.getVar( var.varname, slices=slices ) # Download the data
    if values is None:                                                          # If None
      log.error('Failed to download: {}'.format(var.varname))                           # Log error
      status = False
      break

    # Process/interpolate data so is correct size for writing
    # Because we use the shape of this data to define the size of variable
    # Dimensions, values muse be final shape of data before dimensions are
    # defined.
    fill = atts.pop('_FillValue', None) 
    if isinstance(values, np.ma.core.MaskedArray):                              # If data are masked array  
      fill   = values.fill_value                                                             # Ensure fill is set
      values = values.filled(np.nan)                                            # Fill with NaN
    elif fill is not None:                                                      # Else, if fill is valud
      if 'float' not in values.dtype.name:                                      # Ensure data is float type
        values = values.astype('float32')
      values[ values == fill ] = np.nan

    values = interp.interpolate( values )                                       # Interpolate data
    if fill is not None:                                                        # If fill is set to something
      values[ np.isnan(values) ] = fill                                         # Replace any nan values with original fill value

    dimkwargs = {esdt.lonVar : interp.newLon, esdt.latVar : interp.newLat}      # Keywords to override longitude and latitude data written to file
    atts['dimensions'] = addDimensions(remote, local, values.shape, slices, atts, **dimkwargs)
    values = values.squeeze()                                                   # Squeeze data to remove any dimensions that are only one (1) element wide

    vid  = local.createVariable( var.varname, values.dtype, atts['dimensions'],
                fill_value = fill, **kwargs )                                   # Create variable in local file
    for attName, attVal in atts.items():                                        # Iterate over attributes
      if attName != 'dimensions':                                               # If not the dimensions attribute
        vid.setncattr( attName, attVal )                                        # Copy the attribute
    vid[:] = values                                                             # Write the data

  local.close()                                                                 # Close local file
  remote.close()

  if status is False:
    log.error('Download failed, deleting : {}'.format( localfile ) )
    os.remove( localfile )
    return False

  return True

def addDimensions(remote, local, shape, slices, atts, **kwargs):
  """
  Add missing dimensions to local data file

  Iterate over the dimensions in given variable so that all the requried
  dimensions can be created in the local file

  Arguments:
    remote (PyDAPDataset) : Remote data object
    local (Dataset) : Local netCDF4.dataset object
    shape (tuple) : Shape of downloaded data
    atts (dict) : Dictionary containing variable attributes

  Keywords:
    None.

  Returns:
    tuple : Dimension names

  """

  dims = []
  for i, dimName in enumerate( atts['dimensions'] ):                            # Iterate over dimensions
    if shape[i] == 1: continue                                                  # If dimension is only 1 element, skip it
    dims.append( dimName )                                                      # Append dimension name to dims list
    if dimName not in local.dimensions:                                         # If dimension not in local
      did = local.createDimension( dimName, shape[i] )                          # Create dimension in local file
      if dimName not in local.variables:                                        # If the dimension is not in local variables
        dimVal, dimAtts = remote.getVar( dimName, slices[i] )                   # Download dimension variable
        if dimVal is not None:                                                  # If download success
          vid    = local.createVariable( dimName, dimVal.dtype, (dimName,) )    # Create local variable
          for attName, attVal in dimAtts.items():                               # Iterate over attributes for variable
            if attName != 'dimensions':                                         # If attribute is not dimensions
              vid.setncattr( attName, attVal )                                  # Write attribute to local file
          vid[:] = kwargs.get(dimName, dimVal)                                  # Write data; use data from keyword argument of same name as dimension if exists, else use data downloaded
  return tuple(dims)


