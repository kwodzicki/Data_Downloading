#!/usr/bin/env python
#+
# Name:
#   era_interim_download_main
# Purpose:
#   A python procedure to download a ton of data

if __name__ == "__main__":
  import argparse, os;                          # Import library for parsing
  from datetime import timedelta
  from data_downloading.ECMWF.ERAI.era_interim_download_an_pl  import era_interim_download_an_pl
  from data_downloading.ECMWF.ERAI.era_interim_download_an_sfc import era_interim_download_an_sfc
  from data_downloading.ECMWF.ERAI.era_interim_download_fc_sfc import era_interim_download_fc_sfc
  dir_path = os.path.dirname(os.path.realpath(__file__));    # Get path of the script

  parser = argparse.ArgumentParser(description="ERA-Interim Analisys Download");
  ### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
  parser.add_argument("-o", "--outdir", type=str, help="Top level directory for output. Default is one directory above this script!");
  parser.add_argument("-e", "--email",  type=str, help="email address to send failed message to");
  parser.add_argument("-d", "--delay",  type=int, help="Delay from current date to stop downloading in weeks. E.g., -d 26 does not attempt to download data newer than 26 weeks old.");
                  
  args = parser.parse_args();                        # Parse the arguments
  delay = None if args.delay is None else timedelta(weeks = args.delay)
  if args.outdir is None:                            # If no outdir was input
    args.outdir = os.path.dirname( dir_path ); # Use directory one above the directory this script is in
  status = era_interim_download_an_pl( args.outdir, email = args.email, delay = delay );
  if status != 0:
    print( 'There was an error downloading the pressure level analysis data!' );
    exit(status);
  status = era_interim_download_an_sfc( args.outdir, email = args.email, delay = delay );
  if status != 0:
    print( 'There was an error downloading the surface analysis data!' );
    exit(status);
  status = era_interim_download_fc_sfc( args.outdir, email = args.email, delay = delay );
  if status != 0:
    print( 'There was an error downloading the surface forecast data!' );
    exit(status);
  exit(0)
