#!/usr/bin/env python
#+
# Name:
#   era_download_check_new
# Purpose:
#   A python script to check that data in the newly download ERA-Interim files
#   match that of the old files that had some issues in May, June, July, 
#   August, and September of 1980.
# Inputs:
#   None.
# Outputs:
#   A logging file
# Keywords:
#   DELETE : Set to delete the log file, i.e., start from beginning.
# Author and History:
#   Kyle R. Wodzicki     Created 22 Jun. 2017
#-

import os, glob, sys;
from netCDF4 import Dataset;
import numpy as np;
from send_email import send_email;

email   = 'wodzicki@tamu.edu';
subject = 'era_download_check_new';

delete = True if len(sys.argv) == 2 else False;																	# Set delete
log = os.path.expanduser("~") + '/era_download_check_py.txt';										# Set path to log file

var_skip = ['longitude', 'latitude', 'level', 'time'];													# Variables to skip in the check

old_dir = '/Volumes/Data_Rapp/Wodzicki/ERA_Interim/6H_SRFC-100hPa_1.5/';				# Set path to old files
new_dir = '/Volumes/Data_Rapp/ERA_Interim/';																		# Set path to new files
old_files = glob.glob( os.path.join(old_dir, '*.nc') );													# Find all netCDF files in old directory	
new_files = [];																																	# Initialize new files list
for root, dirs, files in os.walk( new_dir ):																		# Walk through the directories where new files are located
	for file in files:																														# Iterate over all the files
		if file.endswith('.nc') and root != new_dir: 
			new_files.append( os.path.join(root, file) );															# If the file ends in '.nc' and is NOT in the root directory, then append the full path to the new_files list
nOld, nNew = len( old_files ), len( new_files );																# Get number of old and new files
if nOld == 0 or nNew == 0:
	print 'No files!!!'; exit(1);																									# Print message and exit

old_dates = [ (i.split('_')[-1]).split('.')[0] for i in old_files ];						# Get dates from old files
new_dates = [ i.split('_')[-2] for i in new_files ];														# Get dates from new files

lines = '';																																			# Set lines array to empty string
if not os.path.isfile( log ) or delete:
	fid = open( log, 'w', 0 );
else:																																						# Else the file does exist
	with open( log, 'r' ) as fid: lines = fid.readlines();												# Read in all the old data
	fid = open( log, 'a', 0 );																										# Open the file in append mode

for i in range(nOld):																														# Iterate over all the old files
	old_file = old_files[i].replace(old_dir,'');																	# Shortened version of old file
	if any(old_file in line for line in lines): continue;													# Check if the shortened version of old file exists in the lines array; skip the if it does
	new_file_id = [ j for j in range(nNew) if new_dates[j] == old_dates[i] ];			# Indices for new files that have same date as old file
	if len(new_file_id) != 3: continue;																						# If three (3) new files were NOT found, then they have not yet been downloaded

	found, not_found, var_ne = [], [], [];																				# Initialize some lists
	fid.write('old File: ' + old_file + '\n');																		# Write some information to the log file
	
	try:
		old_id = Dataset( old_files[i],  mode = 'r' );																# Open input file in read mode
	except:
		status=send_email(email,subject,'Could NOT open old file');
		break;
	old_id.set_auto_maskandscale( False );																				# Turn off auto scaling
	if 'level' in old_id.variables:
		try:
			old_lvl = old_id.variables['level'][:];																		# attempt to read in the level information
		except:
			status=send_email(email, subject, 'Error reading levels from old file');	# Send an email
			old_id.close();																														# Close the file
			exit(1);
	else:
		old_lvl = [];																																# Set old_lvl variable to empty list
	nold_lvl = len( old_lvl );																										# Number of levels in the old data file
	for j in new_file_id: 																												# Iterate over the new file indices
		new_file = new_files[j].replace(new_dir,'');																# Shortened version of new file
		fid.write('  New File: ' + new_file + '\n');																# Write some information to the log file
		try:
			new_id = Dataset( new_files[j],  mode = 'r' );														# Open input file in read mode
		except:
			status=send_email(email,subject,'Could NOT open new file');
			exit(1);
		new_id.set_auto_maskandscale( False );																			# Turn off auto scaling
		if 'level' in new_id.variables:
			try:
				new_lvl = new_id.variables['level'][:];																	# Attempt to read in the level information
			except:
				status=send_email(email, subject, 'Error reading levels from new file');# Send an email
				old_id.close(); new_id.close();																					# Close the files
				exit(1);
		else:
			new_lvl = [];																															# Set new_lvl variable to empty list
		nnew_lvl = len(new_lvl);																										# Number of levels in the new data file
		for k in old_id.variables:																									# Iterate over all variables in the old file
			if k in var_skip or k in found: continue;																	# If k is one of the variables to skip OR it has been found already, then continue
			if k not in new_id.variables:																							# If the old variable is NOT in the new file
				if k not in found and k not in not_found: not_found.append( k );				# If the old variable is NOT in the found list AND NOT in the not_found list, append variable name to not_found list
			else:																																			# Else, the old variable was found in the new file
				found.append( k );																											# Append the old variable to the found list
				if k in not_found: not_found.remove( k );																# If the variable was previously added to the not_found list, then remove it from the not_found list
				fid.write('    {:12}: {:.<10}'.format('Checking var',k));								# Write some information to the log file
				try:
					old_data = old_id.variables[k][:];																		# Read in the old data
				except:
					status=send_email(email, subject, 'Error reading var from old file');	# Send an email
					old_id.close(); new_id.close();																				# Close the files
					exit(1);
				if old_id.variables[k].ndim != 4:																				# If the data is NOT four (4) dimensional
					try:
						new_data = new_id.variables[k][:];																	# Read in the old data
					except:
						status=send_email(email, subject, 'Error reading var from new file');	# Send an email
						old_id.close(); new_id.close();																			# Close the files
						exit(1);																														# Read in all the new data
				else:																																		# Else, the data is four (4) dimensional
					new_data = np.empty_like( old_data );																	# Initialize new data array
					for n in range( nold_lvl ):																						# Iterate over all the levels in the old file
						lvl_id = [m for m in range( nnew_lvl ) if new_lvl[m] == old_lvl[n]];# Locate level from old data in new data
						try:
							new_data[:,[n],:,:] = new_id.variables[k][:,lvl_id,:,:];					# Read in the old data
						except:
							status=send_email(email, subject, 'Error reading var from new file');	# Send an email
							old_id.close(); new_id.close();																		# Close the files
							exit(1);																													# Read in all the new data

				if np.array_equal(old_data, new_data):																	# If the scaled data are equal
					fid.write('MATCH\n');																									# Write output to log file
				else:																																		# Else, must check the unscaled values
					old_miss  = old_id.variables[k].getncattr('missing_value');						# Get the missing value from the old data
					old_fill  = old_id.variables[k].getncattr('_FillValue');							# Get the fill value from the old data
					old_scale = old_id.variables[k].getncattr('scale_factor');						# Get the scale factor from the old data
					old_add   = old_id.variables[k].getncattr('add_offset');							# Get the add offset from the old data
					new_miss  = new_id.variables[k].getncattr('missing_value');						# Get the missing value from the new data
					new_fill  = new_id.variables[k].getncattr('_FillValue');							# Get the fill value from the new data
					new_scale = new_id.variables[k].getncattr('scale_factor');						# Get the scale factor from the new data
					new_add   = new_id.variables[k].getncattr('add_offset');							# Get the add offset from the new data
					
					old_bad = np.where((old_data == old_miss) | (old_data == old_fill));	# Locate missing and fill values in the old data
					new_bad = np.where((new_data == new_miss) | (new_data == new_fill));	# Locate missing and fill values in the new data
					if np.array_equal(old_bad, new_bad) is False:
						fid.write('\n     Missing inidices missmatch!!!\n');								# Write output to log file
					else:
						old_good = np.where((old_data != old_miss) & (old_data != old_fill));# Locate valid data in the old data
						new_good = np.where((new_data != new_miss) & (new_data != new_fill));# Locate valid data in the new data
						old_data = old_data[old_good] * old_scale + old_add;								# Scale the old data
						new_data = new_data[new_good] * new_scale + new_add;								# Scale the new data
						diff     = np.absolute(old_data - new_data);												# Compute difference between old and new data
						if k == 'z':																												# threshold for z
							test = diff > 6;
						elif k == 'sp':																											# threshold for sp
							test = diff > 50;
						else:																																# threshold for all others
							test = diff > 0.1
						if np.any(test == True):																						# If there is at least one (1) True value in the array
							fid.write('NO MATCH - '+str(max(diff))+'\n');											# Then the arrays do NOT match
						else:																																# Else, all the values are false
							fid.write('MATCH\n');																							# The arrays MATCH
		new_id.close();																															# Close the new file
	fid.write('  Var(s) not found: '+', '.join(not_found)+'\n');									# Write list of variables that were NOT found
	old_id.close();																																# Close the old file
fid.close();																																		# Close the log file
exit(0);