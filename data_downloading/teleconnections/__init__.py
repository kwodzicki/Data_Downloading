import os

URLS = {
  'PDO'  : {'data'     : 'https://www.ncdc.noaa.gov/teleconnections/pdo/data.json',
            'info'     : 'https://www.ncdc.noaa.gov/teleconnections/pdo/'},
  'ENSO' : {'base'     : 'https://www.esrl.noaa.gov/psd/',
            'nino34_1' : 'data/correlation/nina34.data',
            'nino34_2' : 'gcos_wgsp/Timeseries/Data/nino34.long.data',
            'nino4'    : 'data/correlation/nina4.data',                          # ESRL/NOAA, Nino 4, asci
            'nino12'   : 'data/correlation/nina1.data',                          # ESRL/NOAA, Nino 1+2, as
            'oni'      : 'data/correlation/oni.data',                            # ESRL/NOAA, Ocean Nino I
            'tni'      : 'data/correlation/tni.data',                            # ESRL/NOAA, Trans Nino I
            'mei'      : 'enso/mei/data/meiv2.data'}                             # ESRL Multivariate E
}


def getOutDir( outRoot ):
  """Function for consistent directory naming for Teleconnection data"""

  return os.path.join( outRoot, 'Teleconnections' )  


from .getPDO  import getPDO
from .getENSO import getENSO
