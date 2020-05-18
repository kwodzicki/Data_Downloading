#!/usr/bin/env python
#+
# Name:
#   era_interim_download_an_sfc
# Purpose:
#   A python function to download ERA-Interim data at the surface at
#   analysis
# Inputs:
#   None.
# Outputs:
#   netCDF files with both the pressure level and surface data
# Keywords:
#   start_year  : Set the start year. Default is 1979.
#   start_month : Set the start month. Default is 1 (January)
#   email       : Set email address for errors.
# Author and History:
#   Kyle R. Wodzicki     Created 15 Jun. 2017
#-

import os;
from datetime import datetime, timedelta;
from send_email import send_email;
from compress_netcdf_file import compress_netcdf_file;
from download_era_interim import download_era_interim

def era_interim_download_an_sfc(dir, start_year = None, start_month = None, email = None):
	if start_year  is None: start_year  = 1979;
	if start_month is None: start_month = 1;
	
	subject = 'ERA-Interim an_sfc FAILED!';
	out_dir = os.path.join(dir, 'Analysis', 'Surface');

	date        = datetime.now();
	date_str    = ( '_'.join( str(date).split() ) ).split('.')[0];
	date -= timedelta(weeks = 26);																								# Get current date and subtract 26 weeks
	yy, mm, dd = date.year, date.month, date.day;																	# Convert back to calendar date

	vars  = [	31.128,  32.128,  33.128,  34.128,  35.128,  36.128,  37.128, \
					  38.128,  39.128,  40.128,  41.128,  42.128,  53.162,  54.162, \
						55.162,  56.162,  57.162,  58.162,  59.162,  60.162,  61.162, \
						62.162,  63.162,  64.162,  65.162,  66.162,  67.162,  68.162, \
						69.162,  70.162,  71.162,  72.162,  73.162,  74.162,  75.162, \
						76.162,  77.162,  78.162,  79.162,  80.162,  81.162,  82.162, \
						83.162,  84.162,  85.162,  86.162,  87.162,  88.162,  89.162, \
						90.162,  91.162,  92.162, 134.128, 136.128, 137.128, 139.128, \
					 141.128, 148.128, 151.128, 164.128, 165.128, 166.128, 167.128, \
					 168.128, 170.128, 173.128, 174.128, 183.128, 186.128, 187.128, \
					 188.128, 198.128, 206.128, 234.128, 235.128, 236.128, 238.128];																										# List of analysis pressure level variables to download

	info = {"class"    : "ei", 
					"dataset"  : "interim",
					"expver"   : "1", 
					"grid"     : "1.5/1.5",
					"area"     : "90/0/-90/360",
					"stream"   : "oper",
					"levtype"  : "sfc", 
					"type"     : "an",
					"time"     : "00:00:00/06:00:00/12:00:00/18:00:00",
					"step"     : "0",
					"param"    : '/'.join( [ str(i) for i in vars] ),
					"format"	 : "netcdf",
					"target"   : ''};	

	logfile = 'ERAI_download_{}_{}.log'.format(info['type'],info['levtype']);
	logfile = os.path.join(dir, 'logs', logfile)
	
	data = download_era_interim(info, logfile = logfile, 
		verbose = True, netcdf = True);
	
	if os.path.isfile( data.logfile ): os.remove( data.logfile );									# Delete old log file
	while start_year * 100L + start_month <= yy * 100L + mm:
		date = str(start_year * 100L + start_month);																# Set the date
		tmp = '_'.join( [info['type'], info['levtype'], date] );
		data.info['target'] = os.path.join(out_dir, tmp+'.nc');											# Set target file name
		data.set_date(sYear = start_year, sMonth = start_month);

		fmt = '  {:2d}: {:40}\n';																										# Format for status messages in log file
		with open(data.logfile, 'a') as f:
			f.write('Downloading: '+data.info['target']+'\n');												# Print a message

		attempt, max_attempt = 0, 5;																								# Set attempt and maximum attempt for downloading and compressing
		while attempt < max_attempt:																								# Try three times to download and compress the file
 			data.download();																													# Download the data
			f = open( data.logfile, 'a');																							# Open the logging file
			if data.status < 2:																												# If the status returned by the download is less than 2, then the file downloaded and needs compressed
				f.write( fmt.format(attempt+1,"Downloaded!") );													# Print a message
				f.write( fmt.format(attempt+1,"Compressing file...") );									# Print a message
				status = compress_netcdf_file(data.info['target'], email=email, gzip=5, delete=True);# Compress the file
				if status == 0:
					attempt = max_attempt+1;																							# Set attempt to four (4)
					f.write( fmt.format(attempt+1,"Compressed!") );												# Print a message
				else:					 																													# If the return status of the compression failed, then delete the downloaded file, increment the attempt counter and try to download/compress again
					attempt += 1;																													# Increment the attempt
					if status == 1 : 
						msg = "Output file exists and clobber was not set";									# Set the message for status 1
					elif status == 2:
						msg = "Data was NOT written correctly after three (3) attempts";		# Set the message for status 2
					elif status == 3:
						msg = "There was an error reading the data";												# Set the message for status 3
					elif status == 4:
						msg = "Input file doesn't exist";																		# Set the message for status 4
					f.write( fmt.format(attempt+1, msg) );
					if os.path.exists( data.info['target'] ): 
						os.remove( data.info['target'] );																		# IF the download file exists, delete it
			elif data.status == 2:																										# If the return status of the download is 2, then the compressed file already exists
				f.write( fmt.format(attempt+1,"Compressed file already exists!") );			# Print a message
				attempt = max_attempt+1;																								# Set attempt to four
			else:
				if os.path.exists( data.info['target'] ):
					os.remove( data.info['target'] );																			# If any other number was returned, delete the downloaded file IF it exists
				attempt += 1;																														# Increment the attempt
			f.close();																																	# Closet the logging file 
		if attempt == max_attempt:																									# If attempt is equal to three (3), then the file failed to download/compress three times and the program halts
			with open(data.logfile, 'a') as f:
				f.write( fmt.format(attempt+1,"Reached maximum attempts") );						# Print a message
			if email is not None: status = send_email(email, subject);								# Send an email that the download failed
			return 1;																																	# Exit status one (1)

		start_month += 1																														# Increment the month
		if start_month == 13:	start_year, start_month = start_year + 1, 1;					# If the new month is number 13
	return 0;

if __name__ == "__main__":
	import argparse;                                                              # Import library for parsing
	parser = argparse.ArgumentParser(description="ERA-Interim Analisys Pressure Levels Download");    			# Set the description of the script to be printed in the help doc, i.e., ./script -h
	### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
	parser.add_argument("outdir",        type=str, help="Top level directory for output")
	parser.add_argument("-y", "--year",  type=int, help="specifies start year")
	parser.add_argument("-m", "--month", type=int, help="specifies start month")
	parser.add_argument("-e", "--email", type=str, help="email address to send failed message to")
	
	args = parser.parse_args()
	status = era_interim_download_an_sfc( args.outdir, args.year, args.month, args.email );
	exit(status);																																			# Exit status zero (0) on end