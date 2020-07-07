import os;
from datetime import datetime, timedelta;

from ...utils.dateutils import next_month

from ..utils.send_email import send_email;
from ..utils.compress_netcdf_file import compress_netcdf_file;

from .ERAI_Downloader import ERAI_Downloader

ENDDATE = datetime(2019, 8, 1, 0)

class ERAI_General( ERAI_Downloader ):
  def __init__(self, outdir, info = None, subject = None):

    if info is None: info = INFO.copy()

    super().__init__( verbose=True, netcdf=True, **info )

    self.subject = subject
    self.outdir  = outdir

  def download(self, start_year = None, start_month = None, email = None, delay = None):
    '''
    Purpose:
      A function to download all ERA-I analysis variables at surface.
    Inputs:
      dir : Directory to save data to
    Keywords
      start_year  : Year to start looking for data
      start_month : Month to start looking for data
      email       : email address to send error messages to
      delay       : Delay from current date to download until. Must be
                     a timedelta object. Default is to stop downloading
         data when the month and year are within 26 weeks of
         program start date
    '''
    if start_year  is None: start_year  = 1979;
    if start_month is None: start_month = 1;
    if delay       is None: delay       = timedelta(weeks = 26);
    date    = datetime(start_year, start_month, 1)

    while date <= ENDDATE:
      self.set_date( date )
      target = self.defaultTarget()
      if not target:
        print('Issue getting target name')
        return
      self.info['target'] = os.path.join( self.outdir,  target )

      fmt = '  {:2d}: {:40}'                                                   # Format for status messages in log file
      self.log.info('Downloading: '+self.info['target'])                        # Print a message

      attempt, max_attempt = 0, 5;                                                # Set attempt and maximum attempt for downloading and compressing
      while attempt < max_attempt:                                                # Try three times to download and compress the file
        super().download()                                                          # Download the data
        if self.status < 2:                                                       # If the status returned by the download is less than 2, then the file downloaded and needs compressed
          self.log.info( fmt.format(attempt+1,"Downloaded!") )                          # Print a message
          self.log.info( fmt.format(attempt+1,"Compressing file...") )                  # Print a message
          status = compress_netcdf_file(self.info['target'], email=email, gzip=5, delete=True);# Compress the file
          if status == 0:
            attempt = max_attempt+1;                                              # Set attempt to four (4)
            self.log.info( fmt.format(attempt+1,"Compressed!") );                       # Print a message
          else:                                                                   # If the return status of the compression failed, then delete the downloaded file, increment the attempt counter and try to download/compress again
            attempt += 1;                                                         # Increment the attempt
            if status == 1 : 
              msg = "Output file exists and clobber was not set";                 # Set the message for status 1
            elif status == 2:
              msg = "Data was NOT written correctly after three (3) attempts";    # Set the message for status 2
            elif status == 3:
              msg = "There was an error reading the data";                        # Set the message for status 3
            elif status == 4:
              msg = "Input file doesn't exist";                                   # Set the message for status 4
            self.log.info( fmt.format(attempt+1, msg) )
            if os.path.exists( self.info['target'] ): 
              os.remove( self.info['target'] );                                   # IF the download file exists, delete it
        elif self.status == 2:                                                    # If the return status of the download is 2, then the compressed file already exists
          self.log.info( fmt.format(attempt+1,"Compressed file already exists!") )      # Print a message
          attempt = max_attempt+1;                                                # Set attempt to four
        else:
          if os.path.exists( self.info['target'] ):
            os.remove( self.info['target'] );                                      # If any other number was returned, delete the downloaded file IF it exists
          attempt += 1;                                                           # Increment the attempt
      if attempt == max_attempt:                                                  # If attempt is equal to three (3), then the file failed to download/compress three times and the program halts
        self.log.error( fmt.format(attempt+1,"Reached maximum attempts") )            # Print a message
        if email is not None: status = send_email(email, subject);                # Send an email that the download failed
        return 1;                                                                 # Exit status one (1)

      date = next_month(date)
    return 0;

if __name__ == "__main__":
  import argparse;                                                              # Import library for parsing
  parser = argparse.ArgumentParser(description="ERA-Interim Analisys Pressure Levels Download");          # Set the description of the script to be printed in the help doc, i.e., ./script -h
  ### Data storage keywords; https://software.ecmwf.int/wiki/display/UDOC/Data+storage+keywords
  parser.add_argument("outdir",        type=str, help="Top level directory for output")
  parser.add_argument("-y", "--year",  type=int, help="specifies start year")
  parser.add_argument("-m", "--month", type=int, help="specifies start month")
  parser.add_argument("-e", "--email", type=str, help="email address to send failed message to")
  
  args   = parser.parse_args()
  inst   = ERAI_AN_SFC(args.outdir)
  status = inst.download( args.year, args.month, args.email );
  exit(status);                                                                     # Exit status zero (0) on end
