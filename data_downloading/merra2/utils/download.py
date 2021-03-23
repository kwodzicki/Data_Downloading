import logging
import sys

import numpy as np
import xml.etree.ElementTree as ET
import certifi

from urllib.request     import urlopen

from .scaling import scaleFillData

FAILEDFMT = 'Attempt {:2d} of {:2d} - Failed to get {}'
LITTLEEND = sys.byteorder == 'little'
NATIVE    = LITTLEEND and '<' or '>'
SWAPPED   = LITTLEEND and '>' or '<'

def urlJoin( *argv ):
  return '/'.join( [str(i) for i in argv] );

def buildURL( URI, date ):
  return urlJoin( URI, date.strftime('%Y/%m') )

def getDatasetPath(topURL, name = None):
  log = logging.getLogger(__name__);
  req  = urlopen( urlJoin( topURL, 'catalog.xml' ), cafile=certifi.where() );   # Open a request for the file
  xml  = req.read();                                                            # Read the catalog xml file from the OPeNDAP server
  req.close();                                                                  # Close the request
  root = ET.fromstring( xml );                                                  # Parse the XML into a tree in python
  for i in range( len(root) ):                                                  # Iterate over all children of the tree
    if 'dataset' in root[i].tag: break;                                         # If 'dataset' is in the tag of the child, stop looping
  if i < len(root):                                                             # If i is less than the number of children of root
    if name is None:
      return [urlJoin(topURL,child.attrib['ID'].split('/')[-1]) for child in root[i]]; # Return a new url to the path in question
    else:
      for child in root[i]:                                                     # Iterate over the children of root[i]
        if child.attrib['name'] == name:                                        # If the 'name' attribute of the child matches the input name
          return urlJoin(topURL, child.attrib['ID'].split('/')[-1]);            # Return a new url to the path in question
  return None;

def getAttributes( dataset, varName, retries = 3 ):
  log = logging.getLogger(__name__)
  log.info('Getting attributes : {}'.format(varName))
  attempt = 0
  while attempt < retries:
    attempt += 1
    try:
      atts               = dataset[varName].attributes
      atts['dimensions'] = dataset[varName].dimensions
    except:
      log.warning(FAILEDFMT.format(attempt, retries, 'attributes') )
    else:
      return atts
  return None

def getValues( dataset, varName, slices = None, retries = 3 ):
  log     = logging.getLogger(__name__)
  log.info( 'Getting data : {}'.format(varName) )
  attempt = 0
  dims    = dataset[varName].shape
  if slices is None:
    slices = []
    for i in range( len(dims) ):
      slices.append( slice(0, dims[i]) )
    slices = tuple( slices )

  while attempt < retries:
    attempt += 1
    try:
      values = dataset[varName].data[ slices ]
    except:
      log.warning(FAILEDFMT.format(attempt, retries, 'data') )
    else:
      return values
  return None     

def downloadVariable( dataset, varName, 
     slices = None, scaleandfill = False):
  log = logging.getLogger( __name__)

  atts = getAttributes( dataset, varName )
  if atts is not None:
    values = getValues( dataset, varName, slices = slices)
    if values is not None:
      if values.dtype.byteorder == SWAPPED:
        dt     = np.dtype( str(values.dtype).replace(SWAPPED, NATIVE)  )
        values = values.astype( dt )
      if scaleandfill:
        return scaleFillData(values, atts), atts
      else:
        return values, atts
  return None, None

