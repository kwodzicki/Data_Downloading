import numpy as np

class MERRA2Variable():
  def __init__(self, varname, longitude=None, latitude=None, level=None, time=None):
    self.varname   = varname
    self.longitude = longitude
    self.latitude  = latitude
    self.level     = level
    self.time      = time

  def __repr__(self):
    return f'< {self.__class__.__name__} : {self.varname} >'

  def _countOffset(self, ref, data):
    if ref is None or data is None:
      return slice(0, data.shape[0])
    elif isinstance(ref, slice):
      if ref.step is not None:                                                  # If the step is set, assume user forced the step
        return ref                                                              # Just return
      xx = np.where( (data >= ref.start) & (data <= ref.stop) )[0]
      return slice( xx.min(), xx.max()+1 )
    else:
      xx = np.abs( data - ref ).argmin()
      return slice(xx, xx+1)

  def getLonCountOffset(self, lonData=None):
    return self._countOffset( self.longitude, lonData )

  def getLatCountOffset(self, latData=None):
    return self._countOffset( self.latitude, latData )

  def getLevCountOffset(self, levData=None):
    return self._countOffset( self.level, levData )

  def getTimeCountOffset(self, timeData=None):
    return self._countOffset( self.time, timeData )

  def get2DSlices(self, **kwargs):
    """
    Arguments:
      None.

    Keyword arguments:
      lonData (ndarray) : Array of longitude values to use to get slice for
        data downloading
      latData (ndarray) : Array of latitude values to use to get slice for
        data downloading
      levData (ndarray) : Array of level values to use to get slice for
        data downloading
      timeData (ndarray) : Array of time values to use to get slice for
        data downloading

    Returns:
      tuple : Tuple with four (4) slices for downloading

    """

    return ( self.getTimeCountOffset( kwargs.get('timeData', None) ),
             self.getLatCountOffset(  kwargs.get('latData',  None) ),
             self.getLonCountOffset(  kwargs.get('lonData',  None) )
             )

  def get3DSlices(self, **kwargs):
    """
    Arguments:
      None.

    Keyword arguments:
      lonData (ndarray) : Array of longitude values to use to get slice for
        data downloading
      latData (ndarray) : Array of latitude values to use to get slice for
        data downloading
      levData (ndarray) : Array of level values to use to get slice for
        data downloading
      timeData (ndarray) : Array of time values to use to get slice for
        data downloading

    Returns:
      tuple : Tuple with four (4) slices for downloading

    """

    return ( self.getTimeCountOffset( kwargs.get('timeData', None) ),
             self.getLevCountOffset(  kwargs.get('levData',  None) ),
             self.getLatCountOffset(  kwargs.get('latData',  None) ),
             self.getLonCountOffset(  kwargs.get('lonData',  None) )
             )
