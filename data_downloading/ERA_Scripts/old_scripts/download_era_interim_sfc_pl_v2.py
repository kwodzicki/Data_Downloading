#!/usr/bin/env python
#+
# Name:
#   download_era_interim_sfc_pl_v2
# Purpose:
#   An IDL procedure to download the majority of ERA-Interim variables on 
#   model levels and at the surface.
# Inputs:
#   None.
# Outputs:
#   netCDF files with both the pressure level and surface data
# Keywords:
#   VERBOSE : Set to increase verbosity
# Author and History:
#   Kyle R. Wodzicki     Created 15 May 2017
#-

import os;
from datetime import datetime, timedelta;
from send_email import send_email;
from compress_netcdf_file import compress_netcdf_file;
from download_era_interim_v2 import download_era_interim_v2

def download_era_interim_sfc_pl_v2();
	start_year  = 2010;
	start_month = 1;
	date        = datetime.now();
	date_str    = ( '_'.join( str(date).split() ) ).split('.')[0];
	#dir        = '/Volumes/localdata/ERA_Interim/';
	dir         = '/Volumes/Data_Rapp/ERA_Interim/';
	pl_an_dir   = dir + 'Analysis/Pressure_Levels/';
	sfc_an_dir  = dir + 'Analysis/Surface/';
	sfc_fc_dir  = dir + 'Forecast/Surface/';

	pl_an_log  = dir + 'logs/ERAI_download_pl_an_' +date_str+'.log'
	sfc_an_log = dir + 'logs/ERAI_download_sfc_an_'+date_str+'.log'
	sfc_fc_log = dir + 'logs/ERAI_download_sfc_fc_'+date_str+'.log'

	time    	= [ range(0, 24, 6), range(0, 24, 6), [0, 12] ];											# Set initialization times
	step      = [ [0], [0], [6, 12]];																								# Set step times
	leveltype = ['pl', 'sfc', 'sfc'];																							 	# Set level type
	type      = ['an', 'an',  'fc'];																								# Set data type
	out_dirs  = [pl_an_dir, sfc_an_dir, sfc_fc_dir]																	# Set output directories

	grid      = '1.5'

	pl_list = [1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 700, 650, \
							600, 550, 500, 450, 400, 350, 300, 250, 225, 200, 175, 150, 125, \
							100,  50,  10,   5,   1];																						# Set list of model levels to download
	pl_an_var  = [  60.128, 129.128, 130.128, 131.128, 132.128, 133.128, 135.128, \
								 138.128, 155.128, 157.128, 203.128, 246.128, 247.128, 248.128]																												# List of analysis pressure level variables to download
	sfc_an_var = [	31.128,  32.128,  33.128,  34.128,  35.128,  36.128,  37.128, \
									38.128,  39.128,  40.128,  41.128,  42.128,  53.162,  54.162, \
									55.162,  56.162,  57.162,  58.162,  59.162,  60.162,  61.162, \
									62.162,  63.162,  64.162,  65.162,  66.162,  67.162,  68.162, \
									69.162,  70.162,  71.162,  72.162,  73.162,  74.162,  75.162, \
									76.162,  77.162,  78.162,  79.162,  80.162,  81.162,  82.162, \
									83.162,  84.162,  85.162,  86.162,  87.162,  88.162,  89.162, \
									90.162,  91.162,  92.162, 134.128, 136.128, 137.128, 139.128, \
								 141.128, 148.128, 151.128, 164.128, 165.128, 166.128, 167.128, \
								 168.128, 170.128, 173.128, 174.128, 183.128, 186.128, 187.128, \
								 188.128, 198.128, 206.128, 234.128, 235.128, 236.128, 238.128];
	sfc_fc_var = [  44.128,  45.128,  49.128,  50.128, 142.128, 143.128, 144.128, \
								 146.128, 147.128, 159.128, 169.128, 175.128, 176.128, 177.128, \
								 178.128, 179.128, 180.128, 182.128, 205.128, 208.128, 209.128, \
								 210.128, 211.128, 212.128, 228.128, 231.128, 232.128, 239.128, \
								 240.128, 243.128, 244.128];																		# List of forecast surface variables to download

	pl_list = '/'.join( [ str(i) for i in pl_list] );															# Generate pressure levels as forward slash separated list
	vars = [ '/'.join( [ str(i) for i in pl_an_var]  ), \
					 '/'.join( [ str(i) for i in sfc_an_var] ), \
					 '/'.join( [ str(i) for i in sfc_fc_var] ) ];													# Generate list of variables

	const_info = {"class"   : "ei", 
								"dataset" : "interim",
								"expver"  : "1", 
								"grid"    : grid + '/' + grid,
								"area"    : "90/0/-90/360",
								"stream"  : "oper",
								"format"	: 'netcdf'};
	#######
	pl_an_info = const_info.update(
		{"levtype"  : 'pl', 
		 "levelist" : pl_list,
		 "type"     : 'an',
		 "time"     : '00:00:00/06:00:00/12:00:00/18:00:00',
		 "step"     : '0',
		 "param"    : pl_an_var,
		 "target"   : ''});	
	########
	sfc_an_info = const_info.update(
		{"levtype"  : 'sfc', 
		 "type"     : 'an',
		 "time"     : '00:00:00/06:00:00/12:00:00/18:00:00',
		 "step"     : '0',
		 "param"    : sfc_an_var,
		 "target"   : ''});	
	########
	sfc_fc_info = const_info.update(
		{"levtype"  : 'sfc', 
		 "type"     : 'fc',
		 "time"     : '00:00:00/12:00:00',
		 "step"     : '6/12',
		 "param"    : sfc_fc_var,
		 "target"   : ''});	

	pl_an  = download_era_interim_v2(pl_an_info,  logfile = pl_an_log,  verbose = True, netcdf = True)
	sfc_an = download_era_interim_v2(sfc_an_info, logfile = sfc_an_log, verbose = True, netcdf = True)
	sfc_fc = download_era_interim_v2(sfc_fc_info, logfile = sfc_fc_log, verbose = True, netcdf = True)

	x = download( pl_an,  start_year, start_month, date, pl_an_dir, 'an', 'pl',  email ):
	y = download( sfc_an, start_year, start_month, date, pl_an_dir, 'an', 'sfc', email ):
	z = download( sfc_fc, start_year, start_month, date, pl_an_dir, 'fc', 'sfc', email ):

def download( era_class, start_year, start_month, date, out_dir, type, leveltype, email ):
	email   = 'wodzicki@tamu.edu';
	date -= timedelta(weeks = 26);																								# Get current date and subtract 26 weeks
	yy, mm, dd = date.year, date.month, date.day;																	# Convert back to calendar date
	while start_year * 100L + start_month <= yy * 100L + mm:
		date = str(start_year * 100L + start_month);																# Set the date
		era_class.info['target'] = out_dir+'_'.join( [type, leveltype, date] )+'.nc'; # Set target file name
		era_class.info['date']   = '{:4}-{:02}'.format(start_year, start_month);      # Construct date for download
		
		attempt, max_attempt = 0, 5;																								# Set attempt and maximum attempt for downloading and compressing
		while attempt < max_attempt:																								# Try three times to download and compress the file
			era_class.download();																											# Download the data
			if era_class.status < 2:																									# If the status returned by the download is less than 2, then the file downloaded and needs compressed
				status = compress_netcdf_file(file, email=email, gzip=5, delete = True);# Compress the file
				if status == 3: 																												# If the return status of the compression failed, then delete the downloaded file, increment the attempt counter and try to download/compress again
					if os.path.exists( file ): os.remove( file );													# IF the download file exists, delete it
					attempt = attempt + 1;																								# Increment the attempt
				else:
					attempt = max_attempt+1;																							# Set attempt to four (4)
			elif era_class.status == 2:																								# If the return status of the download is 2, then the compressed file already exists
				with open(era_class.logfile, 'a') as f:
					f.write('Compressed file already exists:\n  '+file+'\n');							# Print a message
				attempt = max_attempt+1;																								# Set attempt to four
			else:
				if os.path.exists( file ): os.remove( file );														# If any other number was returned, delete the downloaded file IF it exists
				attempt = attempt + 1;																									# Increment the attempt
		if attempt == max_attempt:																									# If attempt is equal to three (3), then the file failed to download/compress three times and the program halts
			status = send_email(email, subject);																			# Send an email that the download failed
			return 1;																																	# Exit status one (1)

		start_month += 1																															# Increment the month
		if start_month == 13:	start_year, start_month = start_year + 1, 1;						# If the new month is number 13

if __name__ == "__main__":
	import argparse;                                                              # Import library for parsing
	parser = argparse.ArgumentParser(description="Download ERA-Interim");    			# Set the description of the script to be printed in the help doc, i.e., ./script -h
	### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
	parser.add_argument("--target",				type=str, help="specifies a Unix file into which data is to be written after retrieval or manipulation.")

	exit(0);																																			# Exit status zero (0) on end