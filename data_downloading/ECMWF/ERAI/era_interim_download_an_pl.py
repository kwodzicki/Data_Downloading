import os
from datetime import datetime, timedelta 

from .ERAI_General import ERAI_General

LEVELS = [1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 700, 650, \
            600, 550, 500, 450, 400, 350, 300, 250, 225, 200, 175, 150, 125, \
            100,  50,  10,   5,   1];                                           # Set list of model levels to download
VARS   = [  60.128, 129.128, 130.128, 131.128, 132.128, 133.128, 135.128, \
           138.128, 155.128, 157.128, 203.128, 246.128, 247.128, 248.128]                                                       # List of analysis pressure level variables to download

VARS   = '/'.join( [ str(i) for i in VARS] )
LEVELS = '/'.join( [ str(i) for i in LEVELS] )

INFO = {"class"    : "ei", 
        "dataset"  : "interim",
        "expver"   : "1", 
        "grid"     : "1.5/1.5",
        "area"     : "90/0/-90/360",
        "stream"   : "oper",
        "levtype"  : "pl", 
        "type"     : "an",
        "time"     : "00:00:00/06:00:00/12:00:00/18:00:00",
        "step"     : "0",
        "levelist" : LEVELS,
        "param"    : VARS,
        "target"   : ''}; 

def era_interim_download_an_pl(outdir, start_year = None, start_month = None, email = None, delay = None):
  subject = 'ERA-Interim an_pl FAILED!'
  outdir  = os.path.join( outdir, 'Analysis', 'Pressure_Levels');

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
  status = era_interim_download_an_pl( args.outdir, args.year, args.month, args.email );
  exit(status);                                                                     # Exit status zero (0) on end
