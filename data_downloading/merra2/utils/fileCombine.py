import logging

import os

from datetime import datetime, timedelta

import numpy as np

import idlpy

from ..utils.dataScaler import DataScaler

SCALER  = DataScaler()

ATTSKIP = ('scale_factor', 'add_offset', '_FillValue', 'fmissing_value', 'missing_value', 'vmax', 'vmin', 'valid_range')

def timeUnits( refdate = datetime(1900, 1, 1, 0), units='hours' ): 
  return '{} since {}'.format(units, refdate.isoformat(sep=' ', timespec='seconds'))

def parseAtts( var ):
  atts = [] 
  for att in var.ncattrs():
    if att not in ATTSKIP:
      atts.append( att )
  return atts

def copyAtts( src, dst, varName, *args ):
  for arg in args:
    dst[varName].setncattr( arg, src[varName].getncattr( arg ) )
  return

def fileCombine( outfile, files ):
  from netCDF4 import Dataset, date2num, num2date

  log = logging.getLogger(__name__)

  log.info( 'Combining data files for ITCZ' )

  os.makedirs( os.path.dirname( outfile ), exist_ok = True )
  tUnits = timeUnits( ) 
  nFiles = len( files )
  oid    = Dataset(outfile, 'w')                                                # Open output file for writing
  oid.set_auto_maskandscale( False )                                             # Disable auto scaling in output file

  iid    = Dataset(files[0], 'r')                                               # Open first input file for reading

  log.debug( 'Defining dimensions' )
  for dimName in iid.dimensions:                                                # Iterate over all dimensions in input file
    dimsize = iid.dimensions[dimName].size                                      # Get size of dimension
    if dimName == 'time':
      dimsize *= nFiles                                     # If time dimension, multiply by number of files
    _ = oid.createDimension( dimName, dimsize )                                 # Create dimension in output file

  log.debug( 'Creating coordinate variables' )
  allVars  = list( iid.variables.keys() )                                       # Get list of all variables in input file
  varNames = []                                                                 # List of variables that need to be combined
  for varName in allVars:                                                       # Iterate over list of all variables
    if varName not in oid.dimensions:                                           # If the variable is NOT in the list of dimensions
      varNames.append( varName )                                                # Add variable name to list of variables that need to be combined
    else:                                                                       # Else
      vid = oid.createVariable( varName, iid[varName].dtype,
              dimensions = iid[varName].dimensions)                   # Create new variable in output file
      copyAtts( iid, oid, varName, *iid[varName].ncattrs() )          # Copy variable attributes
      if varName == 'time':                                                     # If variable is time
        vid.units = tUnits                                                      # Update time units
      else:
        vid[:] = iid[varName][:]                                      # Copy data from input file to output file


  log.debug( 'Creating data variables' )
  for varName in varNames:
    atts  = parseAtts( iid[varName] )
    vid   = oid.createVariable( varName, SCALER.dtype,
            dimensions = iid[varName].dimensions,
            zlib       = True, 
            fill_value = SCALER._FillValue)
    vid.set_auto_maskandscale( False )                                          # Disable auto scaling in output file
    copyAtts( iid, oid, varName, *atts )
  
  iid.close()                                                                   # Close in the input file

  processTime = True
  for varName in varNames:                                                      # Iterate over variable names for files to combine
    log.debug('Working on : {}'.format(varName) )
    data = np.empty( oid[varName].shape, 
            dtype = iid[varName].dtype )                                        # Initialize large array to read all data into
    tSlice = None
    for i in range( len(files) ):                                               # Iterate over all input files
      iid = Dataset(files[i], 'r')                                              # Open file for reading
      nt  = iid.dimensions['time'].size                                         # Get size of time dimension
      if tSlice is None:                                                        # If no slice is defined
        tSlice = slice(0, nt)                                                   # Initialize slize
      else:                                                                     # Else
        tSlice = slice( tSlice.stop, tSlice.stop + nt )                         # Update the slice

      if processTime:                                                           # If processTime set
        tVar  = iid['time']                                                     # Read in time from file
        dates = num2date( tVar[:], tVar.units )                                 # Scale the time based on units 
        oid['time'][tSlice] = date2num( dates, tUnits )                         # Rescale time and write to output file


      tmp = iid[varName][:]                                                     # Read in variable data
      if data.ndim == 3:                                                        # If 3D
        data[tSlice,:,:] = tmp.filled(np.nan)                                   # Write NaN to masked values and store in data array
      else:                                                                     # Else 4d
        data[tSlice,:,:,:] = tmp.filled(np.nan)

      iid.close()                                                               # Close the input file
    processTime = False                                                         # Set processTime to False; only want to copy data on first run through of varName iteration

    data, scale, offset = SCALER.scaleData( data )                              # Scale the data
    oid[varName].scale_factor = scale                                           # Set scale_factor attribute
    oid[varName].add_offset   = offset                                          # Set add_offset attribute
    oid[varName][:] = data                                                      # Write data to file

  oid.close()                                                                   # Close output file
 
  log.info('Removing input files') 
  for f in files: os.remove(f)
