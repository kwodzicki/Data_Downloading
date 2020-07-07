#!/usr/bin/env python
#+
# Name:
#   compress_netcdf_file
# Purpose:
#   A python module to create a new gzip compressed netCDF4 file
# Inputs:
#   infile : Full path to the file to compress.
# Outputs:
#   A gzip compressed netCDF4 file.
# Return codes:
#   0 : Program finished cleanly
#   1 : Output file exists and clobber was not set
#   2 : Data was NOT written correctly after three (3) attempts.
#   3 : There was an error reading the data
#   4 : Input file doesn't exist
# Keywords:
#   email   : Set to email address to send email to if compression fails.
#   GZIP    : Set this to gzip compression level (1-9). Default is 5.
#   CLOBBER : Set to overwrite compressed output file.
#   REPLACE : Set to replace the input file with the compressed file.
#   DELETE  : Set to delete the orignal file WITHOUT replacement.
#              REPLACE keyword overrides this option.
#   VERBOSE : Set to increase verbosity
# Author and History:
#   Kyle R. Wodzicki     Created 15 May 2017
#
#     Modified 22 May 2017 by Kyle R. Wodzicki
#       Add the delete keyword
#-

import os, sys;
import numpy as np;
from netCDF4 import Dataset;
from .send_email import send_email;
    
FILLVALUE = '_FillValue'

# Function for verbose output
def message( text ):
  print( text, end='' ); sys.stdout.flush();

def compress_netcdf_file(infile, email = None, gzip = None, 
      clobber = False, replace = False, delete = False, verbose = False):
  if not os.path.isfile( infile ): return 4;                                    # If the input file does NOT exist, return code 4
  if gzip is None: gzip = 5;                                                    # Set default gzip level
  if verbose: print( 'Input: ' + infile );                                      # Verbose output  
  outfile = '.'.join(infile.split('.')[:-1]) + '_gzip.nc';                      # Generate output file name
  if verbose: print( 'Output: ' + outfile );                                    # Verbose output  
  if os.path.isfile( outfile ):                                                 # If the output file exists
    if clobber:                                                                 # If the clobber keyword is true
      os.remove( outfile );                                                     # Delete the file
    else:                                                                       # If clobber is false
      if verbose: message( 'Output file exists!\n' );                           # Print verbose output
      return 1;                                                                 # Return code one (1)

  iid = Dataset( infile,  mode = 'r' );                                         # Open input file in read mode
  oid = Dataset( outfile, mode = 'w', format='NETCDF4' );                       # Open output file in write mode and NETCDF4 format
  iid.set_auto_maskandscale( False );                                           # Turn off auto masking and scaling on the input file
  
  # Global attributes
  if verbose: message('  Copying global attributes to new file...');            # Verbose output
  for i in iid.ncattrs():                                                       # Iterate over global attributes in the file
    att = oid.setncattr( i, iid.getncattr(i) );                                 # Write the attribute to the new file
  # Create Dimensions
  if verbose: message('Done!\n  Creating dimensions in new file...');           # Verbose output
  dim_info = {};                                                                # Initialize list to store dimensions pointers and dictionary for dimension info
  for i in iid.dimensions:
    name, size = iid.dimensions[i].name, iid.dimensions[i].size;                # Get the name and size of the dimension
    dim_info.update( { name : size } );                                         # Append dimension info to the dictionary
    dim = oid.createDimension( name, size );                                    # Create new dimension in the output file
    
  # Create Variables
  if verbose: message('Done!\n  Creating variables in new file...');            # Verbose output
  for name in iid.variables:                                                       # Iterate over all variables in the input file
    dims = iid.variables[name].dimensions;                                         # Get variable dimensions
    atts = iid.variables[name].ncattrs()                                           # List attributes for netCDF variable 
    type = np.dtype( iid.variables[name].datatype ).char;                          # Get variable data type
    # Set up chunk sizes
    if len( dims ) == 1:                                                        # If variable is only one (1) dimension
      chunk = [ dim_info[ dims[0] ] ];                                          # For single dimension, chunk size is size of data
    else:
      chunk = [ dim_info[j] for j in dims ];                                    # Set chunk size for each dimension
      chunk[0] = 1;                                                             # Set chunk size for last dimension to one (1)
#     chunk[:-2] = [1] * len(chunk[:-2]);                                       # Set chunk size for dimensions 3-n to one (1)
    if FILLVALUE in atts:
      fill = iid.variables[name].getncattr(FILLVALUE)
    else:
      fill = None
    vid  = oid.createVariable( name, type, dimensions = dims, fill_value=fill, 
      zlib = True, complevel  = gzip, chunksizes = chunk );                     # Create data space in output file

    # Copy attributes
    for att in atts:
      if att != FILLVALUE:
        vid.setncattr( att, iid.variables[name].getncattr(att) );                        # Set attribute in output file

  # Write Variables
  if verbose: message('Done!\n  Writting data...\n');                           # Verbose output
  oid.set_auto_maskandscale( False );                                           # Turn off auto masking and scaling on the output file
  for i in iid.variables:                                                       # Iterate over all variables in the input file
    if verbose: message( '    {:.<20}'.format( i ) );
    try:
      data = iid.variables[i][:];                                               # read in the data from the input file
    except: 
      iid.close(); oid.close();                                                 # Close the input and output files
      os.remove( outfile );                                                     # Delete the output file
      return 3;                                                                 # Return code three if there was an error reading
    attempt = 0;                                                                # Set attempt for data writing
    while attempt < 3:                                                          # Try three (3) times to write the data
      oid.variables[i][:] = data;                                               # Write data to the new file
      oid.sync();                                                               # Force a sync to the output file
      tmp = oid.variables[i][:];                                                # Read in the data that was just written
      attempt = 4 if np.array_equal(data, tmp) else attempt + 1;                # Set attempt to 4 if the data that was written to the new file is equal to the data from the original, else increment attempt
    if attempt == 3:                                                            # If attempt equals three (3) then failed to write the data three (3) times
      if verbose: message( 'Failed!!!\n' );                                     # Verbose output
      if email is not None: status = send_email( email, infile );               # Send an email
      iid.close(); oid.close();                                                 # Close the input and output files
      return 2;                                                                 # Return code two (2)
    elif verbose: message( 'Done!\n' );                                         # Verbose output

  iid.close(); oid.close();                                                     # Close the input and output files
  if replace: 
    os.remove( infile );                                                        # Delete the input file
    os.rename( outfile, infile );                                               # Replace the input file with the output file
  elif delete:
    os.remove( infile );                                                        # Delete the input file

  return 0;                                                                     # Return zero (0) for clean run

# Set up command line arguments for the function
if __name__ == "__main__":
  import argparse;                                                              # Import library for parsing
  parser = argparse.ArgumentParser(description="Compress netCDF file");         # Set the description of the script to be printed in the help doc, i.e., ./script -h

  parser.add_argument("file", type=str, help="Full path to netCDF file to compress."); # Set an option of inputing of a file path. No dictionary can be passed via the command line
  parser.add_argument("-e", "--email",   type=str, help="Email address for email on error.")
  parser.add_argument("-z", "--gzip",    type=int, help="Set gzip compression level (1-9). Default is 5.")
  parser.add_argument("-c", "--clobber", action="store_true", help="Set to overwrite output file.");
  parser.add_argument("-r", "--replace", action="store_true", help="Set to replace input file.");
  parser.add_argument("-d", "--delete",  action="store_true", help="Set to delete input file.");
  parser.add_argument("-v", "--verbose", action="store_true", help="Set for verbose output.");

  args = parser.parse_args();                                                   # Parse the arguments
  return_code = compress_netcdf_file( args.file,
    email   = args.email, 
    gzip    = args.gzip, 
    clobber = args.clobber, 
    replace = args.replace, 
    delete  = args.delete, 
    verbose = args.verbose);
  exit( return_code );
