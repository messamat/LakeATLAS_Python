import arcpy
import os

#Directory structure
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')

#Download utility scripts

utilurl = "https://raw.githubusercontent.com/messamat/HydroATLAS_py/master/utility_functions.py"
request = urllib2.Request(url)
f = urllib2.urlopen(request)
print "downloading " + url

from utility_functions import *

hydrolakesdir = os.path.join(datdir, "hydrolakes")
pathcheckcreate(hydrolakesdir)
dlfile(
    url="https://97dc600d3ccc765f840c-d5a4231de41cd7a15e06ac00b0bcc552.ssl.cf5.rackcdn.com/HydroLAKES_polys_v10.gdb.zip",
    outpath=hydrolakesdir,
    ignore_downloadable=True)