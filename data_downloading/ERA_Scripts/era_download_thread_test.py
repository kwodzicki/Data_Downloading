#!/usr/bin/env python
import datetime, time, os;
import multiprocessing as multi;
from download_era_interim import download_era_interim;

start_year  = 1979;
start_month = 1;
#dir        = '/Volumes/localdata/ERA_Interim/';
dir         = '/Volumes/localdata/ERA_Interim_Test/';
pl_an_dir   = dir + 'Analysis/Pressure_Levels/';
sfc_an_dir  = dir + 'Analysis/Surface/';
sfc_fc_dir  = dir + 'Forcast/Surface/';

email   = 'wodzicki@tamu.edu';
subject = 'ERA-Interim download Failed!';

times    	= [ range(0, 24, 6), range(0, 24, 6), [0, 12] ];											# Set initialization times
step      = [ [0], [0], [6, 12]];																								# Set step times
leveltype = ['pl'];																														 	# Set level type
type      = ['an', 'an',  'fc'];																								# Set data type
out_dirs  = [pl_an_dir, sfc_an_dir, sfc_fc_dir]																	# Set output directories

grid      = '1.5'

date = datetime.date.today() - datetime.timedelta(weeks = 26);									# Get current date and subtract 26 weeks
yy, mm, dd = date.year, date.month, date.day;																		# Convert back to calendar date

pl_list = [1000, 975, 950];																											# Set list of model levels to download
pl_an_var  = [  60.128]																													# List of analysis pressure level variables to download

pl_list = '/'.join( [ str(i) for i in pl_list] );																# Generate pressure levels as forward slash separated list
vars = '/'.join( [ str(i) for i in pl_an_var] );									# Generate list of variables

const_info = {"class"   : "ei", 
							"dataset" : "interim",
							"expver"  : "1", 
							"grid"    : grid + '/' + grid,
							"area"    : "90/0/-90/360",
							"stream"  : "oper",
							"format"	: 'netcdf'};

while start_year * 100L + start_month <= 197901:
	for i in range(0, len(leveltype)):
		date = str(start_year * 100L + start_month);																# Set the date
		file = out_dirs[i] + type[i] + '_' + leveltype[i] + '_' + date + '.nc';			# Set target file name
		date = '{:4}-{:02}'.format(start_year, start_month);												# Construct date for download
		tmpTime = '/'.join( [ '{:02}:00:00'.format( j ) for j in times[i] ] );				# Construct times for download
		tmpStep = '/'.join( [ str( j ) for j in step[i] ] );												# Construct times for download
		
		new_info = {"levtype" : leveltype[i], 
								"type"    : type[i],
								"date"    : '/to/'.join( [date, date] ),
								"time"    : tmpTime,
								"step"    : tmpStep,
								"param"   : vars,
								"target"  : file};																							# Set new options for info
		if leveltype[i] == 'pl': new_info.update( {"levelist" : pl_list} );					# Append the level list to the new options
		
		info = dict( const_info, **new_info )
		attempt, max_attempt = 0, 5;																								# Set attempt and maximum attempt for downloading and compressing
		while attempt < max_attempt:																								# Try three times to download and compress the file
			status = download_era_interim( info, timeout = 0.75 );
# 			elapsed, t0, timeout = 0, time.time(), 180;																# Set elapsed time for download to zero, current time, and timeout to 3 hours (180 minutes)
# 			process = multi.Process( target = download_era_interim, args=( info, ) );
# 			process.start();
# 			while True:
# 				time.sleep(30);																													# Sleep for 10 mintues
# 				print process.is_alive();
# 				if process.is_alive():
# 					if elapsed >= timeout:
# 						process.terminate();																								# Terminate the process
# 						status = 3;																													# Set return status to three (3)
# 						break;																															# Break the while loop
# 				else:
# 					process.join();																												# Exit the process cleanly
# 					status = process.exitcode;																						# Get the exit code from the process
# 					break;																																# Break the while loop
# 				elapsed = (time.time() - t0) / 60;																			# Compute new elapsed time

			print status;
			if status < 2:																														# If the status returned by the download is less than 2, then the file downloaded and needs compressed
				print 'Download success'
				attempt = max_attempt + 1;
			elif status == 2:																													# If the return status of the download is 2, then the compressed file already exists
				print 'Compressed file already exists:\n  '+file;												# Print a message
				attempt = max_attempt+1;																								# Set attempt to four
			else:
				os.remove( file );																											# If any other number was returned, delete the downloaded file
				attempt = attempt + 1;																									# Increment the attempt
		if attempt == max_attempt:																									# If attempt is equal to three (3), then the file failed to download/compress three times and the program halts
			status = send_email(email, subject);																			# Send an email that the download failed
			exit(1);																																	# Exit status one (1)

	start_month += 1																															# Increment the month
	if start_month == 13:	start_year, start_month = start_year + 1, 1;						# If the new month is number 13

exit(0);			