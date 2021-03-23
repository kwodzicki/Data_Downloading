import logging

import numpy as np

def scaleFillData(data, atts, fillValue = None):
  log = logging.getLogger(__name__);
  if '_FillValue' in atts:
    log.debug( 'Searching for Fill Values in data' );
    bad = (data == atts['_FillValue'])
  else:
    log.debug( "No '_FillValue' attribute" );
    bad = None
  if 'missing_value' in atts:
    log.debug( 'Searching for missing values in data' );    
    if bad is None:
      bad = (data == atts['missing_value']);
    else:
      bad = ((data == atts['missing_value']) | bad);
  else:
    log.debug( "No 'missing_value' attribute" );

  if 'scale_factor' in atts and 'add_offset' in atts:
    log.debug( "Scaling to data" );
    data = data * atts['scale_factor'] + atts['add_offset']
  if bad is not None:
    if np.sum(bad) > 0:
      log.debug( 'Replacing missing and fill values with NaN characters' );
      if data.dtype.kind != 'f':
        log.debug( 'Converting data to floating point array' );
        data = data.astype(np.float32);
  data[bad] = np.nan if fillValue is None else fillValue;
  return data   
