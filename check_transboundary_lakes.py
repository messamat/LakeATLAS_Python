import arcpy
from arcpy.sa import *
import os

arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension('Spatial')

#Directory structure
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')

#Input
flowdir = os.path.join(datdir, 'HydroATLAS', 'Flow_directions', 'flow_dir_15s_global.gdb', 'flow_dir_15s')
gadmras = os.path.join(datdir, 'HydroATLAS', 'admin_boundaries_gadm.gdb', 'countries_gadm_v20')
hylakp = os.path.join(datdir, "HydroLAKES_points_v10.gdb", "HydroLAKES_points_v10")


#Select lakes over 100 km2
[f.name for f in arcpy.ListFields(hylakp)]

arcpy.MakeFeatureLayer_management(hylakp, 'lako100', where_clause="Lake_area >= 500")
arcpy.GetCount_management('lako100')

lakadmin_dir = {}
with arcpy.da.SearchCursor('lako100', ['Hylak_id', 'SHAPE@']) as cursor:
    for row in cursor:
        print(row[0])
        ws = Watershed(in_flow_direction_raster= flowdir,
                       in_pour_point_data= row[1])
        wsadm = ExtractByMask(in_raster=gadmras, in_mask_data=ws)
        nadm = arcpy.GetRasterProperties_management(in_raster=wsadm, property_type = 'UNIQUEVALUECOUNT')
        lakadmin_dir[row[0]] = int(nadm.getOutput(0))

len(lakadmin_dir.values())
sum([x > 1 for x  in lakadmin_dir.values()])/float(len(lakadmin_dir))


