#!/usr/bin/env python
#This script will pull ERA-Interim data from Dec. 1997 thru Dec. 2012
#
#One input is accepted and sets the grid interval
#
#List of variables to get
#   129  - Geopotential Height (m^2 s^-2)
#		130  - Temperature (K)
#		131  - Eastward wind component (m s^-1)
#		132  - Westward wind component (m s^-1)
#		135  - Vertical Velocity (Pa s^-1)
#		155  - Divergence (s^-1)
#		157  - Relative Humidity (%)

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

variables = '129.128/130.128/131.128/132.128/135.128/146.128/147.128' #Set variables to get
variables = variables+'/155.128/157.128'
levels    = '200/225/250/300/350/400/450/500/550/600/650/700/750/'    #Set levels to get
levels    = levels+'775/800/825/850/875/900/925/950/975/1000'

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
		
	filename = save_dir+'Interim_Profile_'+yy+mm+'.nc'
	
	if os.path.isfile(filename):
		continue
		
	date = yy+mm+'01/to/'+yy+mm+dd
	time = '00/06/12/18'
	
	from ecmwfapi import ECMWFDataServer
	server = ECMWFDataServer()
	server.retrieve({
		'dataset'       : "interim",
		'stream'        : "oper",
		'type'          : "an",
		'class'         : "ei",
		'date'          : date,
		'time'          : time,
		'levtype'       : "pl",
		'levelist'      : levels,
		'param'         : variables,
		'grid'          : "1.5/1.5",
		'format'        : "netcdf",
		'target'        : filename
	})
