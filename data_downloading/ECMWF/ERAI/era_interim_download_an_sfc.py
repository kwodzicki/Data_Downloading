import os

from .ERAI_General import ERAI_General

VARS  = [ 31.128,  32.128,  33.128,  34.128,  35.128,  36.128,  37.128,
          38.128,  39.128,  40.128,  41.128,  42.128,  53.162,  54.162,
          55.162,  56.162,  57.162,  58.162,  59.162,  60.162,  61.162,
          62.162,  63.162,  64.162,  65.162,  66.162,  67.162,  68.162,
          69.162,  70.162,  71.162,  72.162,  73.162,  74.162,  75.162,
          76.162,  77.162,  78.162,  79.162,  80.162,  81.162,  82.162,
          83.162,  84.162,  85.162,  86.162,  87.162,  88.162,  89.162,
          90.162,  91.162,  92.162, 134.128, 136.128, 137.128, 139.128,
         141.128, 148.128, 151.128, 164.128, 165.128, 166.128, 167.128,
         168.128, 170.128, 173.128, 174.128, 183.128, 186.128, 187.128,
         188.128, 198.128, 206.128, 234.128, 235.128, 236.128, 238.128]                                                   # List of analysis pressure level variables to download
VARS = '/'.join( [ str(i) for i in VARS] )

INFO = {"class"    : "ei", 
        "dataset"  : "interim",
        "expver"   : "1", 
        "grid"     : "1.5/1.5",
        "area"     : "90/0/-90/360",
        "stream"   : "oper",
        "levtype"  : "sfc", 
        "type"     : "an",
        "time"     : "00:00:00/06:00:00/12:00:00/18:00:00",
        "step"     : "0",
        "param"    : VARS,
        "target"   : ''}; 

def era_interim_download_an_sfc(outdir, start_year = None, start_month = None, email = None, delay = None):
  subject = 'ERA-Interim an_sfc FAILED!'
  outdir  = os.path.join(outdir, 'Analysis', 'Surface')

  inst    = ERAI_General(outdir, info = INFO, subject = subject )
  return inst.download( start_year, start_month, email, delay ) 

if __name__ == "__main__":
  import argparse;                                                              # Import library for parsing
  parser = argparse.ArgumentParser(description="ERA-Interim Analysis Surface Download");          # Set the description of the script to be printed in the help doc, i.e., ./script -h
  ### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
  parser.add_argument("outdir",        type=str, help="Top level directory for output")
  parser.add_argument("-y", "--year",  type=int, default=1979, help="specifies start year")
  parser.add_argument("-m", "--month", type=int, default=   1, help="specifies start month")
  parser.add_argument("-e", "--email", type=str, help="email address to send failed message to")
  
  args   = parser.parse_args()
  inst   = erai_interim_download_an_sfc(args.outdir, args.year, args.month, args.email );
  exit(status);                                                                     # Exit status zero (0) on end
