#!/usr/bin/env python
#This script will pull ERA-Interim data from Dec. 1997 thru Dec. 2012
#
#One input is accepted and sets the grid interval
#
#List of variables to get
#    34  - SST (K)
#   146  - Surface Sensible Heat Flux (J m^-2)
#   147  - Surface Latent Heat Flux (J m^-2)
#   159  - Boundary Layer Height (m)
#   165  - 10 m U wind (m s^-1)
#   166  - 10 m V wind (m s^-1)
# Standard atmospheric levels
#   1000/925/850/700/500/400/300/250/200/150/100
import os
import sys

def julian_day_mod(year, month):
	days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
	if (year % 4) == 0:
		days[2] = days[2]+1
	if (year % 100) == 0 and (year % 400) != 0:
		days[2] = days[2]-1
	result = days[month]
	return result

#Set other variables
save_dir  = '/Volumes/Data_Rapp/Wodzicki/ERA_Interim/'                #save dir
save_dir  = save_dir+'Model_Initialize/'                              #Save dir

variables = '34.128/146.128/147.128/159.128/165.128/166.128'          #Set variables to get

#Set date arrays for iteration
year = [2006, 2007, 2007, 2009, 2010, 2010]                           #Initialize year
month= [  12,    1,    2,   12,    1,    2]                           #Initialize month
		
#Check directory not exist, create it
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
      
for date in range(len(year)):
	yy = str(year[date])
	mm = str(month[date])
	dd = str(julian_day_mod(year[date], month[date]))
	if month[date] < 10:
		mm = '0'+mm;
		
	filename = save_dir+'Interim_SRFC_'+yy+mm+'.nc'
	
	if os.path.isfile(filename):
		continue
		
	date = yy+mm+'01/to/'+yy+mm+dd
	time = '00/12'
	step = '06/12'
	
	from ecmwfapi import ECMWFDataServer
	server = ECMWFDataServer()
	server.retrieve({
		'dataset'       : "interim",
		'stream'        : "oper",
		'type'          : "fc",
		'class'         : "ei",
		'date'          : date,
		'time'          : time,
		'step'          : step,
		'levtype'       : "sfc",
		'parm'          : variables,
		'grid'          : "1.5/1.5",
		'format'        : "netcdf",
		'target'        : filename
	})
