#!/usr/bin/env python3
from gzip import GzipFile;
from urllib.request import urlopen;
from datetime import datetime
import io;
import pandas;
import numpy as np;
srcs = {'1' : 'USAF SURFACE HOURLY observation, candidate for merge with NCEI SURFACE HOURLY (not yet merged, failed element cross-checks)',
        '2' : 'NCEI SURFACE HOURLY observation, candidate for merge with USAF SURFACE HOURLY (not yet merged, failed element cross-checks)',
        '3' : 'USAF SURFACE HOURLY/NCEI SURFACE HOURLY merged observation',
        '4' : 'USAF SURFACE HOURLY observation',
        '5' : 'NCEI SURFACE HOURLY observation',
        '6' : 'ASOS/AWOS observation from NCEI',
        '7' : 'ASOS/AWOS observation merged with USAF SURFACE HOURLY observation',
        '8' : 'MAPSO observation (NCEI)',
        'A' : 'USAF SURFACE HOURLY/NCEI HOURLY PRECIPITATION merged observation, candidate for merge with NCEI SURFACE HOURLY (not yet merged, failed element cross-checks',
        'B' : 'NCEI SURFACE HOURLY/NCEI HOURLY PRECIPITATION merged observation, candidate for merge with USAF SURFACE HOURLY (not yet merged, failed element cross-checks',
        'C' : 'USAF SURFACE HOURLY/NCEI SURFACE HOURLY/NCEI HOURLY PRECIPITATION merged observation',
        'D' : 'USAF SURFACE HOURLY/NCEI HOURLY PRECIPITATION merged observation',
        'E' : 'NCEI SURFACE HOURLY/NCEI HOURLY PRECIPITATION merged observation',
        'F' : 'Form OMR/1001 - Weather Burequ city office (keyed data)',
        'G' : 'SAO surface airways observation, pre-1949 (keyed data)',
        'H' : 'SAO surface airways observation, 1965-1981 format/period (keyed data)',
        'I' : 'Climate Reference Network observation',
        'J' : 'Cooperative Network observation',
        'K' : 'Radiation Network observation',
        'L' : 'Data from Climate Data Modernization Program (CDMP) data source',
        'M' : 'Data from National Renewable Energy Laboratory (NREL) data source',
        'N' : 'NCAR / NCEI cooperative effort (various national datasets)',
        'O' : 'Summary observation created by NCEI using hourly observations that may not share the same data source flag',
        '9' : 'Missing'}

typs = {'AERO'  : 'Aerological report',
        'AUST'  : 'Dataset from Australia',
        'AUTO'  : 'Report from an automatic station',
        'BOGUS' : 'Bogus report',
        'BRAZ'  : 'Dataset from Brazil',
        'COOPD' : 'US Cooperative Network summary of day report',
        'COOPS' : 'US Cooperative Network soil temperature report',
        'CRB'   : 'Climate Reference Book data from CDMP',
        'CRN05' : 'Climate Reference Network report, with 5-minute reporting interval',
        'CRN15' : 'Climate Reference Network report, with 15-minute reporting interval',
        'FM-12' : 'SYNOP Report of surface observation form a fixed land station',
        'FM-13' : 'SHIP Report of surface observation from a sea station',
        'FM-14' : 'SYNOP MOBIL Report of surface observation from a mobile land station',
        'FM-15' : 'METAR Aviation routine weather report',
        'FM-16' : 'SPECI Aviation selected special weather report',
        'FM-18' : 'BUOY Report of a buoy observation',
        'GREEN' : 'Dataset from Greenland',
        'MESOH' : 'Hydrological observations from MESONET operated civilian or government agency',
        'MESOS' : 'MESONET operated civilian or government agency',
        'MESOW' : 'Snow observations from MESONET operated civilian or government agency',
        'MEXIC' : 'Dataset from Mexico',
        'NSRDB' : 'National Solar Radiation Data Base',
        'PCP15' : 'US 15-minute precipitation network report',
        'PCP60' : 'US 60-minute precipitation network report',
        'S-S-A' : 'Synoptic, airways, and auto merged report',
        'SA-AU' : 'Airways and auto merged report',
        'SAO'   : 'Airways report (includes record specials)',
        'SAOSP' : 'Airways special report (excluding record specials)',
        'SHEF'  : 'Standard Hydrologic Exchange Format',
        'SMARS' : 'Supplementary airways station report',
        'SOD'   : 'Summary of day report from U.S. ASOS or AWOS station',
        'SOM'   : 'Summary of month report from U.S. ASOS or AWOS station',
        'SURF'  : 'Surface Radiation Network report',
        'SY-AE' : 'Synoptic and aero merged report',
        'SY-AU' : 'Synoptic and auto merged report',
        'SY-MT' : 'Synoptic and METAR merged report',
        'SY-SA' : 'Synoptic and airways merged report',
        'WBO'   : 'Weather Bureau Office',
        'WNO'   : 'Washington Naval Observatory',
        '99999' : 'Missing'}

info = {'USAF'          : {'sid' :   4, 'eid' :  10, 'scale' : None, 'offset' : None},
        'WBAN'          : {'sid' :  10, 'eid' :  15, 'scale' : None, 'offset' : None},
        'YYYYMMDD'      : {'sid' :  15, 'eid' :  23, 'scale' : None, 'offset' : None},
        'HHMM'          : {'sid' :  23, 'eid' :  27, 'scale' : None, 'offset' : None},
        'source'        : {'sid' :  27, 'eid' :  28, 'scale' : None, 'offset' : None},
        'latitude'      : {'sid' :  28, 'eid' :  34, 'scale' : 1000, 'offset' : None},
        'longitude'     : {'sid' :  34, 'eid' :  41, 'scale' : 1000, 'offset' : None},
        'type'          : {'sid' :  41, 'eid' :  46, 'scale' : None, 'offset' : None},
        'elevation'     : {'sid' :  46, 'eid' :  51, 'scale' :    1, 'offset' : None},
        'st_name'       : {'sid' :  51, 'eid' :  56, 'scale' : None, 'offset' : None},
        'qcontrol'      : {'sid' :  56, 'eid' :  60, 'scale' : None, 'offset' : None},
        'wind_dir'      : {'sid' :  60, 'eid' :  63, 'scale' :    1, 'offset' : None},
        'wind_dir_qlt'  : {'sid' :  63, 'eid' :  64, 'scale' : None, 'offset' : None},
        'wind_typ'      : {'sid' :  64, 'eid' :  65, 'scale' : None, 'offset' : None},
        'wind_spd'      : {'sid' :  65, 'eid' :  69, 'scale' :   10, 'offset' : None},
        'wind_spd_qlt'  : {'sid' :  69, 'eid' :  70, 'scale' : None, 'offset' : None},
        'ceil_hgt'      : {'sid' :  70, 'eid' :  75, 'scale' :    1, 'offset' : None},
        'ceil_qlt'      : {'sid' :  75, 'eid' :  76, 'scale' : None, 'offset' : None},
        'ceil_det'      : {'sid' :  76, 'eid' :  77, 'scale' : None, 'offset' : None},
        'ceil_cavok'    : {'sid' :  77, 'eid' :  78, 'scale' : None, 'offset' : None},
        'vis_dist'      : {'sid' :  78, 'eid' :  84, 'scale' : None, 'offset' : None},
        'vis_qlt'       : {'sid' :  84, 'eid' :  85, 'scale' : None, 'offset' : None},
        'vis_vari'      : {'sid' :  85, 'eid' :  86, 'scale' : None, 'offset' : None},
        'vis_vari_qlt'  : {'sid' :  86, 'eid' :  87, 'scale' : None, 'offset' : None},
        'air_temp'      : {'sid' :  87, 'eid' :  92, 'scale' :   10, 'offset' : None},
        'air_temp_qlt'  : {'sid' :  92, 'eid' :  93, 'scale' : None, 'offset' : None},
        'air_dtemp'     : {'sid' :  93, 'eid' :  98, 'scale' :   10, 'offset' : None},
        'air_dtemp_qlt' : {'sid' :  98, 'eid' :  99, 'scale' : None, 'offset' : None},
        'SLP'           : {'sid' :  99, 'eid' : 104, 'scale' :   10, 'offset' : None},
        'SLP_qlt'       : {'sid' : 104, 'eid' : 105, 'scale' : None, 'offset' : None}}
infoORD = ['source', 'wind_dir', 'wind_dir_qlt', 'wind_typ', 'wind_spd',
           'wind_spd_qlt', 'ceil_hgt', 'ceil_qlt', 'ceil_det', 'ceil_cavok',
           'vis_dist', 'vis_qlt', 'vis_vari', 'vis_vari_qlt', 'air_temp',
           'air_temp_qlt', 'air_dtemp', 'air_dtemp_qlt', 'SLP', 'SLP_qlt', 'date']

station_list = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv'
urlFMT  = 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/{}/{}'
dateFMT = '{:4d}{:02d}{:02d}'


def getStationList():
  data = urlopen( station_list ).read();
  file = io.BytesIO( data );
  return pandas.read_csv( file );

def parseGA1_6( lvl, txt ):
  data = {'coverage_code'   : int(txt[:2]),
          'coverage_qlt'    : txt[2:3],
          'base_height'     : float(txt[3:9]),
          'base_height_qlt' : txt[9:10],
          'cloud_type'      : txt[10:12],
          'cloud_type_qlt'  : txt[12:13]}
  if data['coverage_code'] == 99 or data['cloud_type'] == 99:
    return None
  else:
    return data;

nMand = 105;
def parseMandatory( txt ):
  data = {}
  for tag in info:
    val = txt[ info[tag]['sid']:info[tag]['eid'] ];
    if info[tag]['scale']  is not None: val = float(val) / info[tag]['scale']
    if info[tag]['offset'] is not None: val = val + info[tag]['offset']
    data[tag] = val;
  data['date'] = datetime( int(data['YYYYMMDD'][0:4]), int(data['YYYYMMDD'][4:6]),
                           int(data['YYYYMMDD'][6:8]), int(data['HHMM'][0:2]), 
                           int(data['HHMM'][2:4]) )
  data = [ data[tag] for tag in infoORD ]
  return data

def parseAdditional( txt ):
  print(txt[:3]);
  if txt[:3] != 'ADD': return None;
  sid = 3;
  eid = sid + 3;
  while True:
    key = txt[sid:eid];
    sid = eid;
    if 'GA' in key:
      eid = sid + 13;
      data = parseGA1_6( key[-1], txt[sid:eid] );
      print( data );
      break
    sid = eid;
    eid = sid + 3;
  return data

def NOAA_ISD_Parser( fileobj, year, month, day ):
  date = dateFMT.format(year, month, day)
  data = pandas.DataFrame( columns = infoORD );
  iid  = GzipFile( fileobj = fileobj );
  line = iid.readline().decode('ascii');
  while line != '': 
    mand = parseMandatory(  line[:nMand] );
    if mand['date'] == date: 
      print( mand );
#     addi = parseAdditional( line[nMand:] ) 
    line = iid.readline().decode('ascii');
  iid.close();

def downloadData(station, year, month, day):
  stInfo   = getStationList();
  stations = stInfo.loc[ stInfo['ICAO'] == station.upper() ];
  fileFMT  = '{}-{}-{}.gz'
  sdate    = stations['BEGIN'].values * 1.0E-4;
  edate    = stations['END'  ].values * 1.0E-4;
  for i in range( sdate.size ):
    if year >= sdate[i] and year <= edate[i]:
      file = fileFMT.format(
        stations['USAF'].values[i], stations['WBAN'].values[i], year
      )
      break;
  url     = urlFMT.format(year, file)
  print(url)
  fileobj = io.BytesIO( urlopen( url ).read() );
  NOAA_ISD_Parser( fileobj, year, month, day );
  
if __name__ == "__main__":
#   file = '/Users/kwodzicki/Downloads/A51256-00451-2018.gz'
#   NOAA_ISD_Parser( file )
  downloadData( 'kabe', 2018, 10, 9 )