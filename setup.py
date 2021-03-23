#!/usr/bin/env python
from setuptools import setup, convert_path
import os

NAME     = "data_downloading"
DESC     = "Package for downloading various meteorological data sets"
URL      = "https://github.tamu.edu/wodzicki/Data_Downloading"
AUTHOR   = "Kyle R. Wodzicki"
EMAIL    = "wodzicki@tamu.edu"

main_ns  = {}
ver_path = convert_path( os.path.join( NAME, "version.py" ) )
with open(ver_path) as ver_file:
  exec(ver_file.read(), main_ns)

setup(
  name             = NAME,
  description      = DESC,
  url              = URL,
  author           = AUTHOR,
  author_email     = EMAIL, 
  version          = main_ns['__version__'],
  packages         = setuptools.find_packages(),
  install_requires = [ "numpy", "netCDF4", "pydap",
                       "requests", "certifi", "bs4", "lxml", 
                       "ecmwf-api-client",
                       "idlpy @ git+https://github.com/kwodzicki/idlpy"],
  scripts          = [],
  zip_safe         = False,
)

