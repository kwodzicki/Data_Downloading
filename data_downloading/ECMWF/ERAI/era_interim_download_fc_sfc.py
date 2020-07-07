import os

from .ERAI_General import ERAI_General

VARS =  [  44.128,  45.128,  49.128,  50.128, 142.128, 143.128, 144.128,
          146.128, 147.128, 159.128, 169.128, 175.128, 176.128, 177.128,
          178.128, 179.128, 180.128, 182.128, 205.128, 208.128, 209.128,
          210.128, 211.128, 212.128, 228.128, 231.128, 232.128, 239.128,
          240.128, 243.128, 244.128]

VARS = '/'.join( map( str, VARS ) )

INFO = {"class"    : "ei", 
        "dataset"  : "interim",
        "expver"   : "1", 
        "grid"     : "1.5/1.5",
        "area"     : "90/0/-90/360",
        "stream"   : "oper",
        "levtype"  : "sfc", 
        "type"     : "fc",
        "time"     : "00:00:00/12:00:00",
        "step"     : "6/12",
        "param"    : VARS,
        "format"   : "netcdf",
        "target"   : ''}; 



def era_interim_download_fc_sfc(outdir, 
  start_year  = None, 
  start_month = None, 
  email       = None, 
  delay       = None):

  """
  Download all ERA-I forecast variables at surface.

  Arguments:
    dir : Directory to save data to

  Keyword arguments:
    start_year  : Year to start looking for data
    start_month : Month to start looking for data
    email       : email address to send error messages to
    delay       : Delay from current date to download until. Must be
                   a timedelta object. Default is to stop downloading
       data when the month and year are within 26 weeks of
       program start date

  """

  subject = 'ERA-Interim fc_sfc FAILED!';
  outdir  = os.path.join(outdir, 'Forecast', 'Surface');

  inst    = ERAI_General(outdir, info = INFO, subject = subject)
  return inst.download( start_year, start_month, email, delay )

if __name__ == "__main__":
  import argparse;                                                              # Import library for parsing
  parser = argparse.ArgumentParser(description="ERA-Interim Analisys Pressure Levels Download");          # Set the description of the script to be printed in the help doc, i.e., ./script -h
  ### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
  parser.add_argument("outdir",        type=str, help="Top level directory for output")
  parser.add_argument("-y", "--year",  type=int, default=1979, help="specifies start year")
  parser.add_argument("-m", "--month", type=int, default=   1, help="specifies start month")
  parser.add_argument("-e", "--email", type=str, help="email address to send failed message to")
  
  args = parser.parse_args()
  status = era_interim_download_fc_sfc( args.outdir, args.year, args.month, args.email );
  exit(status);                                                                     # Exit status zero (0) on end
