import arcpy
from arcpy.sa import *
import numpy as np
import os

from explode_overlapping import *

arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension('Spatial')

#Directory structure
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')
hylakcheckgdb = os.path.join(resdir, 'mathis_hylak_v11_results.gdb')

#In
hylak = os.path.join(datdir, 'hydrolakes', 'HydroLAKES_polys_v10.gdb', 'HydroLAKES_polys_v10')
landmask = os.path.join(datdir, 'HydroATLAS', 'Masks', 'hydrosheds_landmask_15s.gdb', 'hys_land_15s')
arcpy.env.snapRaster = arcpy.env.cellSize = arcpy.env.extent = landmask

#Out
hylak15ras = os.path.join(hylakcheckgdb, 'HydroLAKES_polys_v10_ras15')
hylakbufdiag = os.path.join(hylakcheckgdb, 'HydroLAKES_polys_v10_buf15')

#Rasterize lake polygons
if not arcpy.Exists(hylak15ras):
    arcpy.PolygonToRaster_conversion(in_features=hylak, value_field='hylak_id',
                                     out_rasterdataset=hylak15ras, cell_assignment='MAXIMUM_COMBINED_AREA',
                                     cellsize = landmask)

#Check number of lakes that were correctly rasterized
if not arcpy.Raster(hylak15ras).hasRAT:
    arcpy.BuildRasterAttributeTable_management(hylak15ras)
arcpy.GetRasterProperties_management(in_raster=hylak15ras, property_type = 'UNIQUEVALUECOUNT').getOutput(0) #1107531
arcpy.GetCount_management(hylak) #1427688
1427688-1107531

#Bufferize HydroLAKES
# if not arcpy.Exists(hylakbufdiag):
#
#     arcpy.Buffer_analysis(hylak, hylakbufdiag, buffer_distance_or_field=bufsize,
#                           line_side='FULL', line_end_type='ROUND',
#                           dissolve_option='NONE', method='PLANAR')

#Explode overlapping lakes into separate layers
arcpy.env.workspace = hylakcheckgdb
bufsize = (2**(0.5)*arcpy.Describe(landmask).meanCellWidth)/2
ExplodeOverlapping(fc=hylak, tolerance=bufsize, keep=True, overwrite=False)

#Get unique sets of non-overlapping lakes
arcpy.env.extent = arcpy.env.snapRaster = landmask
subres = arcpy.Describe(landmask).meanCellWidth/10
gpset = {row[0] for row in arcpy.da.SearchCursor(hylak, 'expl')}
for gp in gpset:
    print(gp)
    hylak_sub = arcpy.MakeFeatureLayer_management(hylak,where_clause= 'expl = {}'.format(gp))
    outras = os.path.join(hylakcheckgdb, 'Hylak_ras_sub{}'.format(gp))
    if not arcpy.Exists(outras):
        arcpy.PolygonToRaster_conversion(in_features=hylak_sub, value_field='Hylak_id', out_rasterdataset=outras,
                                         cell_assignment='CELL_CENTER', cellsize=subres)
