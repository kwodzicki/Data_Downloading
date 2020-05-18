#!/usr/bin/env python
#This script will pull ERA-Interim data from Dec. 1997 thru Dec. 2012
#
#One input is accepted and sets the grid interval
#
#List of variables to get
#		130  - Temperature (K)
#		131  - Eastward wind component (m s^-1)
#		132  - Westward wind component (m s^-1)
#		133  - Specific Humidity (kg kg^-1)
#		135  - Vertical Velocity (Pa s^-1)
#		155  - Divergence (s^-1)
#		157  - Relative Humidity
#		246  - Cloud Liquid Water Content (kg kg^-1)
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

#Set grid interval
if len(sys.argv) == 2:                                    #Check for user input
	grid_int= str(sys.argv[1])                              #Set user grid int
else:
	grid_int  = '1.5'                                       #Set default grid int	

#Set other variables
save_dir  = '/Volumes/localdata/ERA_Interim/';    						#save dir
save_dir  = save_dir+'RH_6H_600-400hPa_'+grid_int+'/';					#Save dir

area      = '90/0/-90/360'                                #Set the domain to use
variables = '157'                     								    #Set variables to get
levels    = '600/550/500/450/400'                         #Set levels to get

#Set date arrays for iteration
year = []                             	                #Initialize year
month= []                                               #Initialize month

for i in range(1979,2015):                                #Iterate over years
	for j in range(1,13):                                   #Iterate over months
		year.append(i)                                        #Append year to array
		month.append(j)                                       #Append month to array
		
#Check directory not exist, create it
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
    
grid = grid_int+'/'+grid_int
    
for date in range(len(year)):
	cur_year = year[date]
	cur_month= month[date]
	dd = julian_day_mod(cur_year, cur_month)
	
	if month[date] < 10:
		mm = '0'+str(cur_month)
	else:
		mm = str(cur_month)
		
	filename = save_dir+'RH_'+str(cur_year)+mm+'.nc'
	
	if os.path.isfile(filename):
		continue
		
	date = str(cur_year)+mm+'01/to/'+str(cur_year)+mm+str(dd)
	
	from ecmwfapi import ECMWFDataServer
	server = ECMWFDataServer()
	server.retrieve({
		'dataset'       : "interim",
		'step'          : "00",
		'time'          : "00/06/12/18",
		'stream'        : "oper",
		'levtype'       : "pl",
		'level'         : levels,
		'date'          : date,
		'origin'        : "all",
		'type'          : "an",
		'parm'          : variables,
		'grid'          : grid,
		'area'          : area,
		'format'        : "netcdf",
		'target'        : filename
	})
