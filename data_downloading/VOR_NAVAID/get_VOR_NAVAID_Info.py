#!/usr/bin/env python

import os, re, io, zipfile, struct
from urllib.request import urlopen
from urllib.parse   import urlparse

# https://www.faa.gov/about/office_org/headquarters_offices/ato/service_units/techops/navservices/transition_programs/vormon/media/VOR_Retention_List.xlsx
URLfmt            = 'https://www.airnav.com/cgi-bin/navaid-info?id={}'
globalAirportURL  = 'https://www.partow.net/downloads/GlobalAirportDatabase.zip'
globalAirportFile = urlparse(globalAirportURL).path.split('/')[-1].split('.')[0]

outFile           = os.path.join( os.path.expanduser('~'), 'GlobalNAVAIDDatabase.dat')
latLon_Pattern    = re.compile(b'(?:Lat\/Long\:.*\(([\d\.\/\-]+)\))');                 # Pattern for extracting lat/lon
elev_Pattern      = re.compile(b'(?:Elevation\:\s*([\d\.]+(?=\s*ft)))');               # Pattern for extracting elevation

zipData  = io.BytesIO( )                                                            # Open BytesIO object
zipData.write( urlopen(globalAirportURL).read() )                                   # Download the GlobalAirportsDatabase zip and write it to the BytesIO object
zipFile  = zipfile.ZipFile( zipData )                                               # Create ZipFile object from BytesIO object
airports = zipFile.open( globalAirportFile+'.txt' ).read().decode().splitlines()    # Open the GlobalAirportsDatabase.txt file in the GlobalAirportsDatabase.zip, read data, decode from bytes, and split lines
zipFile.close()                                                                     # Close zip file
zipData.close()                                                                     # Close zip data

count = 0
outID = open(outFile, 'wb')                                                         # Open output file in binary mode
for airport in airports:                                                            # Iterate over all lines
    ICAO, IATA = airport.split(':')[:2]                                             # Get station id
    if (ICAO == 'N/A') and (IATA == 'N/A'): continue

    URL    = URLfmt.format( IATA )                                                  # Build URL
    try:                                                                            # Try to
        data = urlopen(URL).read()                                                  # Get page from url
    except:                                                                         # On exception
        continue                                                                    # Skip
    latLon = latLon_Pattern.findall( data )                                         # Try to find lat/lon
    elev   = elev_Pattern.findall(   data )                                         # Try to find elevation
    if (len(latLon) == 1) and (len(elev) == 1):                                     # If found lat/lon AND elevation
        count += 1
        print( '{:4d} - ICAO: {}, IATA: {}'.format(count, ICAO, IATA) )
        lat, lon = [float(i) for i in latLon[0].decode().split('/')]                # Decode lat/lon, split on '/', convert to float
        elev     = float(elev[0].decode())                                          # Decode elevation and convert to float
        data = struct.pack('<4s3s3d', bytes(ICAO, 'ascii'), bytes(IATA, 'ascii'), 
                            lat, lon, elev);                                        # Pack data as bytes
        outID.write( data )                                                         # Write binary data to file
outID.close()                                                                       # Close file
