import numpy as np

from idlpy import interpolate

class InterpLonLat():
  def __init__(self, origLon, origLat):
    self.newLon  = None
    self.newLat  = None
    self.dLon    = None
    self.dLat    = None

    self._xint   = None
    self._yint   = None

    self.origLon = origLon
    self.origLat = origLat

  @property
  def origLon(self):
    return self._origLon
  @origLon.setter
  def origLon(self, val):
    if not isinstance(val, np.ndarray):
      val = np.asarray( val )
    self._origLon         = val
    dLon                  = val[1] - val[0]
    self._origLonPad      = np.pad(val, 1, mode='edge')
    self._origLonPad[ 0] -= dLon
    self._origLonPad[-1] += dLon
    if self.dLon is not None:
      self.setLonRes( self.dLon )

  @property
  def origLat(self):
    return self._origLat
  @origLat.setter
  def origLat(self, val):
    if not isinstance(val, np.ndarray):
      val = np.asarray( val )
    self._origLat = val
    if self.dLat is not None:
      self.setLatRes( self.dLat )

  def setLonRes(self, dLon=None):
    """
    Sets longitude resolution for interpolated data

    Longitude array for output resolution is also created

    Arguments:
      dLon (float) : Set longitude resolution for interpolated data

    Returns:
      None.

    """
    self.dLon   = dLon
    if dLon is None:
      self.newLon = self.origLon
    else:
      self.newLon = np.arange( 360.0 / dLon ) * dLon + self.origLon.min()
    origLon     = self._origLonPad
    self._xint  = np.interp(self.newLon, origLon, np.arange(origLon.size))

  def setLatRes(self, dLat=None):
    """
    Sets latitude resolution for interpolated data

    Latitude array for output resolution is also created

    Arguments:
      dLat (float) : Set latitude resolution for interpolated data

    Returns:
      None.

    """

    self.dLat   = dLat 
    if dLat is None:
      self.newLat = self.origLat
    else:
      self.newLat = np.arange( (180.0 / dLat) + 1 ) * dLat
      if self.origLat[0] > self.origLat[1]:
        self.newLat = 90.0 - self.newLat
      else:
        self.newLat = self.newLat - 90.0

    self._yint = np.interp(self.newLat, self.origLat, np.arange(self.origLat.size))

  def setLonLatRes(self, dLon, dLat):
    """
    Sets longitude/latitude resolution for interpolated data

    Both longitude and latitude arrays for output resolution are 
    also created when the dimensions are set

    Arguments:
      dLon (float) : Set longitude resolution for interpolated data
      dLat (float) : Set latitude resolution for interpolated data

    Returns:
      None.

    """

    self.setLonRes( dLon )
    self.setLatRes( dLat )

  def interpolate(self, data):
    """
    Interpolate data from original resolution to output resolution

    Data input are assumed to be at the resolution set at class 
    initialization or whatever resoltion is currently in the 
    self.origLon and self.origLat attributes.

    We assume date being interpolated are global and so data
    at the edges of longitude (assumed last dimension) are wrapped so
    that interpolation near edges is accurate.

    Arguments:
      data (numpy.ndarray) : Array of data to interpolate

    Returns:
      numpy.ndarray : Interpolated data

    """

    if self._xint is None:
      raise Exception('Must set output longitude resolution first!') 
    if self._yint is None:
      raise Exception('Must set output latitude resolution first!') 

    if self.dLon is None and self.dLat is None:                                 # If dLon and dLat are NOT set, then do NOT interpolate
      return data
 
    pad_width = [ (1, 1) ]
    for i in range( 1, data.ndim ):
      pad_width.append( (0, 0) )
    pad_width = pad_width[::-1]
    data      = np.pad(data, pad_width, mode='wrap')

    oldShape = None
    if data.ndim > 3:
      oldShape = data.shape
      newShape = ( np.product(oldShape[:-2]), *oldShape[-2:] )
      data     = data.reshape( newShape )
  
    zint = np.arange(data.shape[0])
    data = interpolate(data, zint, self._yint, self._xint)
    
    if oldShape is not None:
      return data.reshape( *oldShape[:-2], *data.shape[-2:] )
    return data

#  yint = np.interp(newLat, origLat, np.arange(origLat.size))
#  xint = np.interp(newLon, origLon, np.arange(origLon.size))


