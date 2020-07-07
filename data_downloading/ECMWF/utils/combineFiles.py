import logging
import os
import numpy as np
from netCDF4 import Dataset

outdir       = os.path.expanduser('~')

################################################################################
def combineFiles( *args, 
					 date        = None, 
					 outfile     = None,
					 description = None,
					 gzip        = 5,
					 delete      = False):
  '''
  Name:
    combineFiles
  Purpose:
    A python function to combine data in two netCDF files into one file
    with gzip compressed variables
  Inputs:
    Files to combine
  Outputs:
    Returns True if finished
  Keywords:
    year    : Year of all the files
    month   : Month of all the files
  '''
  log = logging.getLogger(__name__)
  if (date is None): 
    raise Exception('Must input date!')

  for arg in args:
    if not os.path.isfile( arg ):
      log.error('Input file does NOT exist: {}'.format(arg))
      return False

  if outfile is None:
    outfile = fileFMT.format( date.strftime('%Y%m'), 'combine' );
    outfile = os.path.join( outdir, outfile );
  if not os.path.isdir( os.path.dirname(outfile) ): 
    os.makedirs( os.path.dirname(outfile) )

  oid    = Dataset( outfile, mode = 'w', format = 'NETCDF4' )
  oid.set_auto_maskandscale( False )
  if (description is not None):
    oid.setncattr( 'description', description ) 
  
  for arg in args:                                                              # Iterate over all input files
    log.info( 'Copying data from: {}'.format( arg ) )
    iid = Dataset( arg, mode = 'r' )
    iid.set_auto_maskandscale( False );

    dim_info = {};                                                              # Initialize dictionary for dimension info																																# Initialize list to store dimensions pointers and dictionary for dimension info
    for i in iid.dimensions:
      name, size = iid.dimensions[i].name, iid.dimensions[i].size;              # Get the name and size of the dimension
      dim_info.update( { name : size } );					# Append dimension info to the dictionary
      if size == 1: continue
      if name not in oid.dimensions:						# If dimension does NOT exist in output file
        dim = oid.createDimension( name, size );				# Create new dimension in the output file

    for i in iid.variables:							# Iterate over all variables in the input file
      name = iid.variables[i].name;						# Get variable name
      if name in oid.variables:	    						# If the variable does NOT exist in the output file
        log.info( '  Variable exists in new file: {}'.format( name ) )
      else:
        log.info( '  Creating variable: {}'.format( name ) )
        dims  = list( iid.variables[i].dimensions );    			# Get variable dimensions
        dtype = np.dtype( iid.variables[i].datatype ).char;			# Get variable data type
        # Set up chunk sizes
        if len( dims ) == 1:							# If variable is only one (1) dimension
          chunk = [ dim_info[ dims[0] ] ];				        # For single dimension, chunk size is size of data
        else:
          chunk = []
          ddims = []
          while len(dims) > 0:
            dd = dims.pop(0)
            if (dim_info[dd] > 1):
              chunk.append( dim_info[dd] )
              ddims.append( dd )
          dims = ddims
        if (len(chunk) > 2): chunk[:-2] = [1] * len(chunk[:-2]);                # Set chunk size for dimensions 3-n to one (1)

        vid = oid.createVariable( name, dtype, dimensions = dims, 
	    		zlib = True, complevel  = gzip, chunksizes = chunk );	# Create data space in output file
        vid.set_auto_maskandscale(False)
		  # Copy attributes
        for j in iid.variables[i].ncattrs():					# Iterate over variable attributes in the input file
          vid.setncattr( j, iid.variables[i].getncattr(j) );			# Set attribute in output file
       
        log.info( '  Reading/writing in variable: {}'.format( name ) )
        data   = iid.variables[i][:];					        # Read in data from input file
        vid[:] = data;		                # Write data to output file
        
    iid.close();

  oid.close();

  if delete:
    for arg in args:
      os.remove( arg )
