#!/usr/bin/env python
from setuptools import setup, convert_path

main_ns  = {};
ver_path = convert_path("ECMWF/version.py");
with open(ver_path) as ver_file:
  exec(ver_file.read(), main_ns);

setup(
  name             = "Data_Downloading",
  description      = "Package for downloading various data",
  url              = "https://github.tamu.edu/wodzicki/Data_Downloading",
  author           = "Kyle R. Wodzicki",
  author_email     = "wodzicki@tamu.edu",
  version          = main_ns['__version__'],
  packages         = setuptools.find_packages(),
  install_requires = [ "bs4", "lxml", "ecmwfapi", "netCDF4" ],
  scripts          = [],
  zip_safe         = False,
);

