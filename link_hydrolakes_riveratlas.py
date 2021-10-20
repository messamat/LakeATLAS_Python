import arcpy
from arcpy.sa import *
import os

arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension('Spatial')

#Directory structure
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')
hylakcheckgdb = os.path.join(resdir, 'mathis_hylak_v11_results.gdb')

#In
hylakp = os.path.join(datdir, "HydroLAKES_points_v10.gdb", "HydroLAKES_points_v10")
riveratlas_idras = os.path.join(datdir, "HydroATLAS", "Link_zone_grids", "link_stream.gdb", "link_str_pol")
riverlink = os.path.join(datdir, "HydroATLAS", "Link_shapefiles", "link_hyriv_v10.gdb", "link_hyriv_v10")

grdcp = os.path.join(os.path.splitdrive(rootdir)[0],
                     "//globalIRmap", "results", "spatialoutputs.gdb", "grdcstations_riverjoinedit")

#Out
linkpoltab = os.path.join(hylakcheckgdb, 'hylakp_link_str_pol')
linkidtab = os.path.join(resdir, "link_hyriv_v10.csv")
grdctab = os.path.join(resdir, 'grdcstations_riverjoinedit.csv')

#Extract HydroRIVERS ID within their immediate catchment
if not arcpy.Exists(linkpoltab):
    Sample(in_rasters = riveratlas_idras, in_location_data=hylakp, out_table=linkpoltab, resampling_type="NEAREST",
           unique_id_field = "Hylak_id")
    arcpy.CopyRows_management(linkpoltab, os.path.join(resdir,
                                                    "{}.csv".format(os.path.splitext(os.path.split(linkpoltab)[1])[0]))
                              )

if not arcpy.Exists(linkidtab):
    arcpy.CopyRows_management(riverlink, linkidtab)

##### GRAB GRDC STATIONS PROCESSED FOR GLOBAL IR MAP #######
if not arcpy.Exists(grdctab):
    arcpy.CopyRows_management(grdcp, grdctab)



