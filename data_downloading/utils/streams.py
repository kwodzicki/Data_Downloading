from datetime import datetime

def getMerraStream( date ):
  """
  Get data stream prefix for files based on date

  See page 19 of https://gmao.gsfc.nasa.gov/pubs/docs/Bosilovich785.pdf
  for more information

  Arguments:
    date (datetime) : Date corresponding to data 

  Keywords:
    None.

  Returns:
    str : Prefix for file with stream; e.g., MERRA2_100

  """

  if date >= datetime(2011, 1, 1, 0):
    stream = 400
  elif date >= datetime(2001, 1, 1, 0):
    stream = 300
  elif date >= datetime( 1992, 1, 1, 0):
    stream = 200
  else:
    stream = 100
  return 'MERRA2_{}'.format(stream)
