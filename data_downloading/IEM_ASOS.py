#!/usr/bin/env python

import os, time, datetime, json;
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

SERVICE = 'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?'

class IEM_ASOS( object ):
  def __init__(self, max_attempts = 6, verbose = False):
    self.max_attempts = max_attempts;
    self.verbose      = verbose
    self.uri    = 'https://mesonet.agron.iastate.edu/geojson/network/{}.geojson'
    self.states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 
                   'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 
                   'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 
                   'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 
                   'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 
                   'VT', 'WA', 'WI', 'WV', 'WY'];
    self._startFMT = 'year1=%Y&month1=%m&day1=%d&';
    self._endFMT   = 'year2=%Y&month2=%m&day2=%d&';

  ##############################################################################
  def getData( self, outdir, startDate, endDate, file = None ):
    if not os.path.isdir( outdir ): os.makedirs( outdir );
    startDate = datetime.datetime(2012, 8, 1)
    endDate   = datetime.datetime(2012, 9, 1)

    service  = [SERVICE, "data=all", "tz=Etc/UTC", "format=onlycomma",
                 "latlon=yes", "report_type=2", 
                 startDate.strftime( self._startFMT ),
                 endDate.strftime(   self._endFMT   )]
    service  = '&'.join( service );

    if file is None:
      stations = self.get_stations_from_networks()
    else:
      stations = self.get_stations_from_filelist( file )
    for station in stations:
      uri = '{}&station={}'.format( service, station );
      if self.verbose: print( 'Downloading: {}'.format(station) );
      data = self.__downloadURI(uri)
      return data;
      if data is not None:
        outfn = '{}_{}_{}.txt'.format(station, startDate.strftime("%Y%m%d%H%M"),
                                  endDate.strftime("%Y%m%d%H%M"))
        with open(outfn, 'w') as out: out.write(data);
  ##############################################################################
  def get_stations_from_filelist(self, filename):
    """
    Build a listing of stations from a simple file listing the stations.
    The file should simply have one station per line.
    """
    stations = [ line.strip() for line in open(filename) ];
    return stations;
  ##############################################################################
  def get_stations_from_networks(self):
    """Build a station list by using a bunch of IEM networks."""
    stations = []
    networks = ['AWOS'];                                                        # IEM quirk to have Iowa AWOS sites in its own labeled network
    for state in self.states: networks.append( "{}_ASOS".format(state) );       # Append information to the networks list for each state
    for network in networks:                                                    # Iterate over all the networks
      uri   = self.uri.format( network );                                       # Build uri for the data
      jdict = self.__downloadURI( uri, True );                                       # Get JSON information for the URI
      if jdict is not None:                                                     # If the JSON download was a success
        for site in jdict['features']:                                          # Iterate over all sites in under the features tag
          stations.append( site['properties']['sid'] );                         # Set the sid of each site and append to the stations list
    return stations;                                                            # Return the stations list
  ##############################################################################
  def __outFile( self, dir, station, sdate, edate ):
    sdate = sdate.strftime( "%Y%m%d%H%M" )
    edate = edate.strftime( "%Y%m%d%H%M" )
    file = '{}_{}_{}.txt'.format(station, sdate, edate);
    return os.path.join( dir, file );
  ##############################################################################
  def __downloadURI(self, uri, JSON = False):
    for i in range(self.max_attempts):                                          # Iterate for the maximum number of attempts; returns will break the loop
      try:                                                                      # Try to...
        request = urlopen(uri, timeout=300);                                    # Open the URI, read the data, and convert it to utf-8
      except Exception as exp:                                                  # On exception...
        print( "download_data({}) failed with {}}".format(uri, exp) );          # Print an error warning
        time.sleep(5);                                                          # Sleep for 5 seconds
      try:
        if JSON:
          data = json.load( request )
        else:
          data = request.read().decode('utf-8');
          if data.startswith('ERROR'): data = None                              # If there were no other errors in the download
      except Exception as exp:                                                  # On exception...
        print( "download_data({}) failed with {}}".format(uri, exp) );          # Print an error warning
        time.sleep(5);                                                          # Sleep for 5 seconds
      request.close();
      return data;
    return None;                                                                # If made it here, failed ALL times, return None

  ##############################################################################
#   def __downloadURI(self, uri):
#     for i in range(self.max_attempts):                                          # Iterate for the maximum number of attempts; returns will break the loop
#       try:                                                                      # Try to...
#         data = urlopen(uri, timeout=300).read().decode('utf-8');                # Open the URI, read the data, and convert it to utf-8
#       except Exception as exp:                                                  # On exception...
#         print( "download_data({}) failed with {}}".format(uri, exp) );          # Print an error warning
#         time.sleep(5);                                                          # Sleep for 5 seconds
#       else:                                                                     # Else...download was success so...
#         if data is not None and not data.startswith('ERROR'):                   # If there were no other errors in the download
#           return data;                                                          # Return the data
#     return None;                                                                # If made it here, failed ALL times, return None
  ##############################################################################
#   def __downloadJSON(self, uri):
#     for i in range(self.max_attempts):                                          # Iterate for the maximum number of attempts; returns will break the loop
#       try:                                                                      # Try to...                                     
#         jdict = json.load( urlopen(uri) );                                      # Open the URI and load the JSON dat
#       except Exception as exp:                                                  # On exception...
#         print( "download_data({}) failed with {}}".format(uri, exp) );          # Print an error warning
#         time.sleep(5);                                                          # Sleep for 5 seconds
#       else:                                                                     # Else... the download was a success
#         return jdict                                                            # Return the jdict variable
#     return None;                                                                # If made it here, failed ALL times, return None

if __name__ == '__main__':
    IEM_ASOS().getData(1,2)