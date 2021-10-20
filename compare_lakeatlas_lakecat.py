from utility_functions import *
import os

arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension('Spatial')

#Directory structure
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')

lakecatdir = os.path.join(datdir, 'lakecat')
pathcheckcreate(lakecatdir)

hylak = os.path.join(datdir, 'hydrolakes', 'HydroLAKES_polys_v10.gdb', 'HydroLAKES_polys_v10')
nhdgdb = os.path.join(datdir, 'NHDPlusNationalData', 'NHDPlusV21_National_Seamless_Flattened_Lower48.gdb')
nhdlak = os.path.join(nhdgdb, 'NHDSnapshot', 'NHDWaterbody')
nhdtab = os.path.join(resdir, 'NHDPlusV21_Waterbody.csv')

#Out
hydronhd_gdb = os.path.join(resdir, 'lakeatlas_nhd_analysis.gdb')
if not arcpy.Exists(hydronhd_gdb):
    arcpy.CreateFileGDB_management(out_folder_path=os.path.split(hydronhd_gdb)[0],
                                   out_name=os.path.split(hydronhd_gdb)[1])

nhdlak_subdiss = os.path.join(hydronhd_gdb, 'nhdlak_subdiss')
hydronhd_inters = os.path.join(hydronhd_gdb, 'hydrolakes_nhdwb_inters')
hydronhd_interstab = os.path.join(resdir, 'hydrolakes_nhdwb_inters.csv')

#Download NHDV2 that is used with LakeCat
if not os.path.exists(nhdgdb):
    nhdplus_url = 'https://s3.amazonaws.com/edap-nhdplus/NHDPlusV21/Data/NationalData/NHDPlusV21_NationalData_Seamless_Geodatabase_Lower48_07.7z'
    dlfile(url=nhdplus_url, outpath=datdir, outfile=os.path.split(nhdplus_url)[1])

#Downdload LakeCat
schema_url = 'ftp://newftp.epa.gov/EPADataCommons/ORD/NHDPlusLandscapeAttributes/LakeCat/FinalTables/schema.ini' #Download schema.ini file
if not os.path.exists(os.path.join(lakecatdir, 'USCensus2010.csv')):
    lakecatcensus_url = "https://gaftp.epa.gov/epadatacommons/ORD/NHDPlusLandscapeAttributes/LakeCat/FinalTables/USCensus2010.zip"
    dlfile(url=lakecatcensus_url, outpath=lakecatdir, outfile=os.path.split(lakecatcensus_url)[1],
           verifycertif=False, ignore_downloadable=True)

#Export NHD waterbody table
arcpy.AddGeometryAttributes_management(nhdlak, 'AREA_GEODESIC', Area_Unit='SQUARE_KILOMETERS')
arcpy.AlterField_management(nhdlak, 'AREA_GEO', 'AREA_nhd', 'AREA_nhd')
arcpy.AddGeometryAttributes_management(nhdlak, 'CENTROID_INSIDE')

if not arcpy.Exists(nhdtab):
    arcpy.CopyRows_management(nhdlak, nhdtab)

#Remove ice masses
uftype = {row[0] for row in arcpy.da.SearchCursor(nhdlak, 'FTYPE')}
nhdlak_sub = arcpy.MakeFeatureLayer_management(nhdlak, where_clause="NOT FTYPE ='Ice Mass'")

#Join NHDplus to HydroLAKES based on intersection % + area agreement
print('Intersecting...')
if not arcpy.Exists(hydronhd_inters):
    hylak_us = arcpy.MakeFeatureLayer_management(hylak, 'hylak_us', where_clause="Country = 'United States of America'")
    arcpy.AddGeometryAttributes_management(hylak_us, 'AREA_GEODESIC', Area_Unit='SQUARE_KILOMETERS')
    arcpy.AlterField_management(hylak_us, 'AREA_GEO', 'AREA_hydrolakes', 'AREA_hydrolakes')
    arcpy.CopyRows_management(hylak_us, os.path.join(resdir, 'hylak_ustab.csv'))

    arcpy.Intersect_analysis([hylak_us, nhdlak_sub], hydronhd_inters, join_attributes='ALL')
    arcpy.AddGeometryAttributes_management(hydronhd_inters, 'AREA_GEODESIC', Area_Unit='SQUARE_KILOMETERS')
    arcpy.AlterField_management(hydronhd_inters, 'AREA_GEO', 'AREA_inters', 'AREA_inters')

# Export to table for analysis in R
arcpy.CopyRows_management(hydronhd_inters, hydronhd_interstab)

