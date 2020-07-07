import logging
import os, re;

from datetime import datetime, timedelta
import numpy as np
from netCDF4 import Dataset
import cdsapi;

from ECMWF.ERA5.ERA5_Downloader import ERA5_Downloader
from ECMWF.utils.combineFiles import combineFiles
outdir       = '/Volumes/flood3/ERA5/Analysis/Pressure_Levels'
#outdir       = '/Users/kwodzicki/ERA5/Analysis/Pressure_Levels'
fileFMT      = 'an_pl_{}_{}.nc'
data_type    = 'reanalysis-era5-pressure-levels';
data_type2   = 'reanalysis-era5-single-levels-monthly-means'

request      = {
  'format'       : 'netcdf',
  'grid'         : [1.5, 1.5]
}

uv_wind_vars = ['131', '132']
uv_wind_lvls = [850, 875, 900, 925, 950, 975, 1000]

temp_rh_vars = ['157',   '130']
temp_rh_lvls = [850]

precip_vars  = 'total_precipitation'

times        =  ['{:02d}:00'.format( t ) for t in range(0, 24, 6) ]

uv_wind_lvls = [str(z) for z in uv_wind_lvls]
temp_rh_lvls = [str(z) for z in temp_rh_lvls]

if not os.path.isdir( outdir ): os.makedirs( outdir )

def downloadITCZData( start_date, end_date, delete = False):
  log = logging.getLogger(__name__)
  c   = ERA5_Downloader() 
  sdate = start_date
  while (sdate < end_date): 
    try:
      edate = datetime(sdate.year, sdate.month+1, 1)
    except:
      edate = datetime(sdate.year+1, 1, 1)
    edate -= timedelta( days = 1 )
    log.info( 'Start date: {}, end date: {}'.format(sdate, edate) )

    outfile = fileFMT.format( sdate.strftime('%Y%m'), 'uv-T-RH-precip' )
    outfile = os.path.join( outdir, outfile )
    
    if os.path.isfile( outfile ): 
      log.info( 'File exists: {}'.format(outfile) )
    else:
      log.info( 'Attempting to create file: {}'.format(outfile) )

      request['year']         = sdate.strftime('%Y')
      request['month']        = sdate.strftime( '%m' )
      request['time']         = times[0] 
      request['product_type'] = 'monthly_averaged_reanalysis' 
      # Download for precipitation
      request['variable']  = precip_vars
      precip_file = fileFMT.format(sdate.strftime('%Y%m'), 'precip')
      precip_file = os.path.join(outdir, precip_file)
      if not os.path.isfile( precip_file ):
        log.info('Downloading precip data...')
        c.retrieve( data_type2, request, precip_file )
      request.pop('year') 
      request.pop('month') 

 
      request['date'] = '{}/{}'.format( sdate.strftime('%Y-%m-%d'),
                                        edate.strftime('%Y-%m-%d') )
      request['time']         = times
      request['product_type'] = 'reanalysis',
      # Download for wind data
      request['variable']       = uv_wind_vars
      request['pressure_level'] = uv_wind_lvls
      uv_file = fileFMT.format(sdate.strftime('%Y%m'), 'uv-wind')
      uv_file = os.path.join( outdir, uv_file )
      if not os.path.isfile( uv_file ):
        log.info('Downloading wind data...')
        c.retrieve( data_type, request, uv_file )
      
      # Download for wind data
      request['variable']       = temp_rh_vars
      request['pressure_level'] = temp_rh_lvls
      T_RH_file = fileFMT.format(sdate.strftime('%Y%m'), 'T-RH')
      T_RH_file = os.path.join(outdir, T_RH_file)
      if not os.path.isfile( T_RH_file ):
        log.info('Downloading temp/RH data...')
        c.retrieve( data_type, request, T_RH_file )
      request.pop('date') 

      c.wait()
      combineFiles( uv_file, T_RH_file, precip_file, 
				delete  = delete,
				date    = sdate, 
            outfile = outfile, 
            description = 'All data except total preciptition are 6-hourly. ' + \
                          'u- and v-wind components are at various pressure levels. ' + \
                          'Temperature and RH are at 850 hPa. ' + \
                          'Total precipitation is the total monthly mean total precipitation')

    sdate = edate + timedelta( days = 1 )
  c._quit()
