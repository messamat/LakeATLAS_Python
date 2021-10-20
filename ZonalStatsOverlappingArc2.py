#Before anything, GDAL needs to be installed to be able to use this tool
#Here a useful link to proceed with the installation, the whole process takes only 10-15 minutes:
#       http://pythongisandstuff.wordpress.com/2011/07/07/installing-gdal-and-ogr-for-python-on-windows/
#       When following these instructions, instead of writing your path to GDAL at the end of your Path variable
#       put it at the beginning
#       If ever there is a conflict between ArcGIS and GDAL (e.g. the module is not recognized when called in a python
#       script from ArcMap, you can install Python Bindings produced by ESRI here
#       (it says 1.8 but it worked for me with 1.11):
#       http://www.arcgis.com/home/item.html?id=1eec30bf5fa042a5a2227b094db89441
#

import gdal
import ogr
import osr
import os
import math
import numpy
import sys
import time
import arcpy
from arcpy import env
from arcpy.sa import *
import arcgisscripting
import csv
import sys

ogr.UseExceptions()

#Sets the maximum cache size used by GDAL
gdal.SetCacheMax(10000000)

class DuplicateError(Exception):
    pass

#Set up the timing of the processes
import cProfile, pstats, StringIO
pr = cProfile.Profile()
pr.enable()

# Raster dataset
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')
input_value_raster = os.path.join(resdir, 'eegtopo_slope')
# Vector dataset(zones)
input_zone_polygon = os.path.join(resdir, 'mathis_hylak_v11.gdb', 'mathis_hylak_v11')
# Output csv file
csv_folder = os.path.join(resdir, 'hylak_v11_statstabs')
stat = 'MEAN'
exist_buf = os.path.join(resdir, 'mathis_hylak_v11_buf100.shp')
ID_field = "Mathis_id"
buf_size = ""
stat_list = stat.split(";")


def loop_zonal_stats(input_zone_polygon, input_value_raster, stat_list, buf_size, exist_buf):

    #Calculate buffers
    #Set env
    #Allow output overwrite
    arcpy.overwriteOutput = True
    arcpy.AddMessage("input_value_raster is " + str(input_value_raster))
    raster = gdal.Open(input_value_raster)
    arcpy.AddMessage("Opened " + str(raster))

    #If the user inputted a buffer size
    if buf_size is not "":
        #Interprets the list that is returned by the tool as a list in Python
        buf_size_list = buf_size.split(";")
        #Iterate process for every buffer size
        for buffer_size in buf_size_list:
            arcpy.AddMessage("Creating" + str(buffer_size))
            #Starts the clock
            tic = time.clock()
            #Generate an output buffer name based on buffer size
            out_buffer = "buffer" + str(buffer_size)
            #Generates buffer based on input size.
            arcpy.Buffer_analysis(input_zone_polygon, out_buffer, str(buffer_size) + " meters", "OUTSIDE_ONLY", "ROUND","NONE")
            #Ends clock
            toc = time.clock()
            #Calculates processing time t1-t0
            tictoc = toc - tic
            arcpy.AddMessage("Successfully created " + out_buffer + " in " + str(tictoc) + " seconds")

            #Opens shapefile with ogr
            shp = ogr.Open(env.workspace + "/" + out_buffer + ".shp")
            #Create a feature layer
            lyr = shp.GetLayer()

            #Implement the division of the large dataset into smaller chunks to speed up
            #Generate search cursor, the arcpy module object to move through records in a table
            tic = time.clock()
            rows = arcpy.SearchCursor(buffer_size, sort_fields = ID_field)
            #Calls SearchCursor.next() to read the first row
            row = rows.next()
            #Set up a placeholder for the dictionary of IDs
            dic_id = {}
            #Set up the first value of the dictionary key
            key = 0
            #Start a loop that will exit when there are no more rows available
            while row:
                #Get the ID of the feature and append it to the following key in the ID dictionary
                dic_id[key] = row.getValue(ID_field)
                #Re-write the "key" variable with by the next higher integer to not overwrite the previous ID value
                key = key + 1
                #Call SearchCursor.next() to move on to the next row
                row = rows.next()
            toc = time.clock()
            tictoc = toc - tic
            arcpy.AddMessage("Created a dictionary of ID values in " + str(tictoc) + "s")

            #Determines the number of features to be extracted from the main dataset in each slice. Choosing that number
            #depends on the memory of the computer. The more and faster the memory, the bigger the slice
            slice_size = 500
            #Count the number of features in the main layer
            lyr_count = len(dic_id)
            arcpy.AddMessage("There are " + str(lyr_count) + " features in the entire dataset")

            #Makes sure the chosen ID field does not have duplicates, cancels analysis if it does

            if len(set(dic_id.values())) == lyr_count:
                #Set up placeholders for the statistics dictionaries
                MEAN = {}
                RANGE = {}
                MIN = {}
                MAX = {}
                STD = {}
                SUM = {}

                #Make sure that there is a need for slicing the dataset
                if lyr_count > slice_size:
                    #Create a list of lower and upper bound keys to be able to later on slice the dataset
                    key_seq = range(0, lyr_count-1, slice_size)
                    #In the case where the number of features in the dataset is not divisible by the slice size, the key_seq list will not have the last slice until the maximum key value
                    #therefore, need to add the maximum value to the list.
                    key_seq.append(lyr_count-1)

                    #The for loop returns two consecutive numbers of the list everytime starting from the first
                    #for min_FID, max_FID in zip(featList, featList[1:]):
                    for key, key2 in zip(key_seq, key_seq[1:]):
                        #Get the lowerbound ID of the slice (independent of whether it is an FID or any kind of unique ID
                        min_FID = dic_id[key]
                        #Get the upperbound ID of the slice (independent of whether it is an FID or any kind of unique ID
                        max_FID = dic_id[key2]
                        arcpy.AddMessage("min_ID=" + str(min_FID) + " , max_ID=" + str(max_FID))
                        #Creates the SQL
                        sql = ID_field + ' >= ' + str(min_FID) + ' AND ' + ID_field + ' < ' + str(max_FID)
                        #toc = time.clock()
                        #tictoc = toc - tic
                        #arcpy.AddMessage("sql took " + str(tictoc) + " s")

                        tic = time.clock()
                        #Empties variable for not running into variable rewriting issues
                        sub_shp = None
                        #Check whether a temporary feature slice already exists and deletes it
                        if arcpy.Exists("C:\\temp\\test.shp"):
                            arcpy.Delete_management("C:\\temp\\test.shp")
                        #Create a temporary shapefile of the feature slice bound by the lower and upperbound
                        arcpy.FeatureClassToFeatureClass_conversion(buffer_size, "C:\\temp", "test", sql)
                        #Open the temporary shapefile using GDAL OGR
                        sub_shp = ogr.Open("C:\\temp\\test.shp")
                        #Create a layer of the slice
                        sub_lyr = sub_shp.GetLayer()
                        toc = time.clock()
                        tictoc = toc - tic
                        #arcpy.AddMessage("Created and opened temp shapefile in" + str(tictoc) + " s" + str(sub_lyr.GetFeatureCount()))

                        tic = time.clock()

                        range_max = slice_size

                        for stats in stat_list:
                            for FID in range(0, range_max):
                                try:
                                    feat = sub_lyr.GetFeature(FID)
                                    #Get raster georeference info
                                    transform = raster.GetGeoTransform()
                                    xOrigin = transform[0]
                                    yOrigin = transform[3]
                                    pixelWidth = transform[1]
                                    pixelHeight = transform[5]

                                    # Get extent of feat
                                    geom = feat.GetGeometryRef()
                                    # Fetch pointer to feature geometry
                                    if (geom.GetGeometryName() == 'MULTIPOLYGON'):
                                        count = 0
                                        pointsX = [];
                                        pointsY = []

                                        for polygon in geom:
                                            #Runs through every polygon of the multipolygon feature
                                            geomInner = geom.GetGeometryRef(count)
                                            ring = geomInner.GetGeometryRef(0)
                                            numpoints = ring.GetPointCount()
                                            for p in range(numpoints):
                                                lon, lat, z = ring.GetPoint(p)
                                                pointsX.append(lon)
                                                pointsY.append(lat)
                                            count += 1
                                    elif (geom.GetGeometryName() == 'POLYGON'):
                                        ring = geom.GetGeometryRef(0)
                                        numpoints = ring.GetPointCount()
                                        pointsX = []
                                        pointsY = []
                                        for p in range(numpoints):
                                            lon, lat, z = ring.GetPoint(p)
                                            pointsX.append(lon)
                                            pointsY.append(lat)

                                    else:
                                        sys.exit()

                                    xmin = min(pointsX)
                                    xmax = max(pointsX)
                                    ymin = min(pointsY)
                                    ymax = max(pointsY)
                                    # Specify offset and rows and columns to read
                                    xoff = int((xmin - xOrigin) / pixelWidth)
                                    yoff = int((yOrigin - ymax) / pixelWidth)
                                    xcount = int((xmax - xmin) / pixelWidth) + 1
                                    ycount = int((ymax - ymin) / pixelWidth) + 1

                                    # Create memory target raster
                                    target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, gdal.GDT_Byte)
                                    target_ds.SetGeoTransform((
                                        xmin, pixelWidth, 0,
                                        ymax, 0, pixelHeight,
                                    ))


                                    # Create for target raster the same projection as for the value raster
                                    raster_srs = osr.SpatialReference()
                                    raster_srs.ImportFromWkt(raster.GetProjectionRef())
                                    target_ds.SetProjection(raster_srs.ExportToWkt())


                                    # Rasterize zone polygon to raster
                                    gdal.RasterizeLayer(target_ds, [1], sub_lyr, burn_values=[1])


                                    # Read raster as arrays
                                    banddataraster = raster.GetRasterBand(1)
                                    dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

                                    bandmask = target_ds.GetRasterBand(1)
                                    datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)

                                    # Mask zone of raster and NoData values
                                    zonerast = numpy.ma.masked_array(dataraster, numpy.logical_not(datamask))
                                    nodata_value = banddataraster.GetNoDataValue()
                                    zoneraster = numpy.ma.masked_equal(zonerast, nodata_value)

                                    ID = dic_id[key + FID]


                                    #Calculate statistics of zonal raster
                                    if stats == 'MEAN':
                                        nummean = numpy.mean(zoneraster)
                                        arcpy.AddMessage(str(nummean))
                                        MEAN[ID] = nummean

                                    elif stats == 'MIN':
                                        nummin = numpy.amin(zoneraster)
                                        MIN[ID] = nummin

                                    elif stats == 'MAX':
                                        nummax = numpy.amax(zoneraster)
                                        MAX[ID] = nummax
                                    elif stats == 'RANGE':
                                        numrange = numpy.ptp(zoneraster)
                                        RANGE[ID] = numrange
                                    elif stats == 'STD':
                                        numstd = numpy.std(zoneraster)
                                        STD[ID] = numstd
                                    elif stats == 'SUM':
                                        numsum = numpy.sum(zoneraster)
                                        SUM[ID] == numsum

                                except Exception as e:
                                    arcpy.AddMessage("An error occured with " + str(dic_id[FID]))
                                    print e.message
                                    pass

                        toc = time.clock()
                        tictoc = toc - tic

                else:
                    sub_lyr = lyr
                    key = 0
                    range_max = lyr_count
                    tic = time.clock()

                    for stats in stat_list:
                        for FID in range(0, range_max):
                            try:
                                feat = sub_lyr.GetFeature(FID)
                                #Get raster georeference info
                                #FID = feat.GetFID()
                                transform = raster.GetGeoTransform()
                                xOrigin = transform[0]
                                yOrigin = transform[3]
                                pixelWidth = transform[1]
                                pixelHeight = transform[5]

                                # Get extent of feat
                                geom = feat.GetGeometryRef()
                                # Fetch pointer to feature geometry
                                if (geom.GetGeometryName() == 'MULTIPOLYGON'):
                                    count = 0
                                    pointsX = [];
                                    pointsY = []

                                    for polygon in geom:
                                        #Runs through every polygon of the multipolygon feature
                                        geomInner = geom.GetGeometryRef(count)
                                        ring = geomInner.GetGeometryRef(0)
                                        numpoints = ring.GetPointCount()
                                        for p in range(numpoints):
                                            lon, lat, z = ring.GetPoint(p)
                                            pointsX.append(lon)
                                            pointsY.append(lat)
                                        count += 1
                                elif (geom.GetGeometryName() == 'POLYGON'):
                                    ring = geom.GetGeometryRef(0)
                                    numpoints = ring.GetPointCount()
                                    pointsX = []
                                    pointsY = []
                                    for p in range(numpoints):
                                        lon, lat, z = ring.GetPoint(p)
                                        pointsX.append(lon)
                                        pointsY.append(lat)

                                else:
                                    sys.exit()

                                xmin = min(pointsX)
                                xmax = max(pointsX)
                                ymin = min(pointsY)
                                ymax = max(pointsY)
                                # Specify offset and rows and columns to read
                                xoff = int((xmin - xOrigin) / pixelWidth)
                                yoff = int((yOrigin - ymax) / pixelWidth)
                                xcount = int((xmax - xmin) / pixelWidth) + 1
                                ycount = int((ymax - ymin) / pixelWidth) + 1

                                # Create memory target raster
                                target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, gdal.GDT_Byte)
                                target_ds.SetGeoTransform((
                                    xmin, pixelWidth, 0,
                                    ymax, 0, pixelHeight,
                                ))


                                # Create for target raster the same projection as for the value raster
                                raster_srs = osr.SpatialReference()
                                raster_srs.ImportFromWkt(raster.GetProjectionRef())
                                target_ds.SetProjection(raster_srs.ExportToWkt())


                                # Rasterize zone polygon to raster
                                gdal.RasterizeLayer(target_ds, [1], sub_lyr, burn_values=[1])


                                # Read raster as arrays
                                banddataraster = raster.GetRasterBand(1)
                                dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

                                bandmask = target_ds.GetRasterBand(1)
                                datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)

                                # Mask zone of raster and NoData values
                                zonerast = numpy.ma.masked_array(dataraster, numpy.logical_not(datamask))
                                nodata_value = banddataraster.GetNoDataValue()
                                zoneraster = numpy.ma.masked_equal(zonerast, nodata_value)

                                ID = dic_id[key + FID]


                                #Calculate statistics of zonal raster
                                if stats == 'MEAN':
                                    nummean = numpy.mean(zoneraster)
                                    arcpy.AddMessage(str(nummean))
                                    MEAN[ID] = nummean

                                elif stats == 'MIN':
                                    nummin = numpy.amin(zoneraster)
                                    MIN[ID] = nummin

                                elif stats == 'MAX':
                                    nummax = numpy.amax(zoneraster)
                                    MAX[ID] = nummax
                                elif stats == 'RANGE':
                                    numrange = numpy.ptp(zoneraster)
                                    RANGE[ID] = numrange
                                elif stats == 'STD':
                                    numstd = numpy.std(zoneraster)
                                    STD[ID] = numstd
                                elif stats == 'SUM':
                                    numsum = numpy.sum(zoneraster)
                                    SUM[ID] == numsum


                            except Exception as e:
                                arcpy.AddMessage("An error occured with " + str(dic_id[FID]))
                                print e.message
                                pass

                #Write the stats in a csv file
                #This code is adapted from http://stackoverflow.com/questions/22273970/writing-multiple-python-dictionaries-to-csv-file
                dicts = []
                for stat in stat_list:
                    if stat=='MEAN' :
                        dicts.append(MEAN)
                    elif stat=='RANGE':
                        dicts.append(RANGE)
                    elif stat== 'MIN':
                        dicts.append(MIN)
                    elif stat== 'MAX':
                        dicts.append(MAX)
                    elif stat== 'STD':
                        dicts.append(STD)
                    elif stat== 'SUM':
                        dicts.append(SUM)

                headers = ["ID"]
                headers.extend(stat_list)


                with open(csv_folder + '/' + out_buffer + '.csv','wb') as ofile:
                #The with statement allows to open the file and close it afterwards regardless of exceptions
                    writer = csv.writer(ofile, delimiter=',')
                    writer.writerow(headers)
                    for key in dicts[0].iterkeys():
                        writer.writerow([key] + [d[key] for d in dicts])
            else :
                arcpy.AddMessage("Duplicate values in ID field")
                sys.exit()
        return None
    else:
        arcpy.AddMessage("No new buffer created")

####################################################################################################################################
####################################################################################################################################

    if exist_buf is not "":
        buf_list = exist_buf.split(";")
        for buffer_size in buf_list:
            arcpy.env.overwriteOutput = True
            #Loop over buffer and/or lakes whether it is a shapefile or in a GDB
            bufpath, buffile = os.path.split(buffer_size)
            arcpy.AddMessage("path: " + str(bufpath) + ", file: " + str(buffile))
            #

            #Opens buffer feature class and create a feature layer
            if arcpy.Describe(os.path.split(buffer_size)[0]).dataType == 'Workspace':
                driver = ogr.GetDriverByName("FileGDB")
                ds = driver.Open(os.path.split(buffer_size)[0], 0)
                lyr = ds.GetLayer(os.path.split(buffer_size)[1])
            else:
                shp = ogr.Open(buffer_size)
                lyr = shp.GetLayer()

                arcpy.AddMessage("Opened buffer feature class")
                arcpy.AddMessage("Got " + str(lyr))

            #Implement the division of the large dataset into smaller chunks to speed up
            #Generate search cursor, the arcpy module object to move through records in a table
            tic = time.clock()
            rows = arcpy.SearchCursor(buffer_size, sort_fields = ID_field)
            #Calls SearchCursor.next() to read the first row
            row = rows.next()
            #Set up a placeholder for the dictionary of IDs
            dic_id = {}
            #Set up the first value of the dictionary key
            key = 0
            #Start a loop that will exit when there are no more rows available
            while row:
                #Get the ID of the feature and append it to the following key in the ID dictionary
                dic_id[key] = row.getValue(ID_field)
                #Re-write the "key" variable with by the next higher integer to not overwrite the previous ID value
                key = key + 1
                #Call SearchCursor.next() to move on to the next row
                row = rows.next()
            toc = time.clock()
            tictoc = toc - tic
            arcpy.AddMessage("Created a dictionary of ID values in " + str(tictoc) + "s")

            #Determines the number of features to be extracted from the main dataset in each slice. Choosing that number
            #depends on the memory of the computer. The more and faster the memory, the bigger the slice
            slice_size = 500
            #Count the number of features in the main layer
            lyr_count = len(dic_id)

            max_id = max(dic_id)

            arcpy.AddMessage("There are " + str(lyr_count) + " features in the entire dataset")

            #Makes sure the chosen ID field does not have duplicates, cancels analysis if it does

            if len(set(dic_id.values())) == lyr_count:
                #Set up placeholders for the statistics dictionaries
                MEAN = {}
                RANGE = {}
                MIN = {}
                MAX = {}
                STD = {}
                SUM = {}

                #Make sure that there is a need for slicing the dataset
                if lyr_count > slice_size:
                    #Create a list of lower and upper bound keys to be able to later on slice the dataset
                    key_seq = range(0, lyr_count-1, slice_size)
                    #In the case where the number of features in the dataset is not divisible by the slice size, the key_seq list will not have the last slice until the maximum key value
                    #therefore, need to add the maximum value to the list.

                    key_seq.append(lyr_count-1)

                    #Sort the feature class before subsetting it
                    sorted_feat = "C:\\temp\\buffer_sort.shp"
                    arcpy.Sort_management(buffer_size, sorted_feat,[[ID_field, "ASCENDING"]])

                    #The for loop returns two consecutive numbers of the list everytime starting from the first
                    #for min_FID, max_FID in zip(featList, featList[1:]):
                    for key, key2 in zip(key_seq, key_seq[1:]):
                        #Get the lowerbound ID of the slice (independent of whether it is an FID or any kind of unique ID
                        min_FID = dic_id[key]
                        #Get the upperbound ID of the slice (independent of whether it is an FID or any kind of unique ID
                        max_FID = dic_id[key2]

                        arcpy.AddMessage("keymin" + str(key) + "keymax" + str(key2) + "min_ID=" + str(min_FID) + " , max_ID=" + str(max_FID))
                        #Creates the SQL
                        arcpy.AddMessage("key2 " + str(key2))
                        arcpy.AddMessage("final key " + str(lyr_count - 1))
                        if key2 < (lyr_count-1):
                            sql = ID_field + ' >= ' + str(min_FID) + ' AND ' + ID_field + ' < ' + str(max_FID)
                        else:
                            sql = ID_field + ' >= ' + str(min_FID) + ' AND ' + ID_field + ' <= ' + str(max_FID)
                        tic = time.clock()

                        #Empties variable for not running into variable rewriting issues
                        sub_shp = None
                        #Check whether a temporary feature slice already exists and deletes it
                        if arcpy.Exists("C:\\temp\\test.shp"):
                            arcpy.Delete_management("C:\\temp\\test.shp")
                        #Create a temporary shapefile of the feature slice bound by the lower and upperbound
                        arcpy.FeatureClassToFeatureClass_conversion(sorted_feat, "C:\\temp", "test", sql)


                        #Open the temporary shapefile using GDAL OGR
                        sub_shp = ogr.Open("C:\\temp\\test.shp")
                        #Create a layer of the slice
                        sub_lyr = sub_shp.GetLayer()

                        tic = time.clock()
                        range_max = slice_size

                        for stats in stat_list:
                            for FID in range(0, range_max):
                                try:
                                    feat = sub_lyr.GetFeature(FID)
                                    #Get raster georeference info
                                    #FID = feat.GetFID()
                                    transform = raster.GetGeoTransform()
                                    xOrigin = transform[0]
                                    yOrigin = transform[3]
                                    pixelWidth = transform[1]
                                    pixelHeight = transform[5]

                                    # Get extent of feat
                                    geom = feat.GetGeometryRef()
                                    # Fetch pointer to feature geometry
                                    if (geom.GetGeometryName() == 'MULTIPOLYGON'):
                                        count = 0
                                        pointsX = [];
                                        pointsY = []

                                        for polygon in geom:
                                            #Runs through every polygon of the multipolygon feature
                                            geomInner = geom.GetGeometryRef(count)
                                            ring = geomInner.GetGeometryRef(0)
                                            numpoints = ring.GetPointCount()
                                            for p in range(numpoints):
                                                lon, lat, z = ring.GetPoint(p)
                                                pointsX.append(lon)
                                                pointsY.append(lat)
                                            count += 1
                                    elif (geom.GetGeometryName() == 'POLYGON'):
                                        ring = geom.GetGeometryRef(0)
                                        numpoints = ring.GetPointCount()
                                        pointsX = []
                                        pointsY = []
                                        for p in range(numpoints):
                                            lon, lat, z = ring.GetPoint(p)
                                            pointsX.append(lon)
                                            pointsY.append(lat)

                                    else:
                                        sys.exit()

                                    xmin = min(pointsX)
                                    xmax = max(pointsX)
                                    ymin = min(pointsY)
                                    ymax = max(pointsY)
                                    # Specify offset and rows and columns to read
                                    xoff = int((xmin - xOrigin) / pixelWidth)
                                    yoff = int((yOrigin - ymax) / pixelWidth)
                                    xcount = int((xmax - xmin) / pixelWidth) + 1
                                    ycount = int((ymax - ymin) / pixelWidth) + 1

                                    # Create memory target raster
                                    target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, gdal.GDT_Byte)
                                    target_ds.SetGeoTransform((
                                        xmin, pixelWidth, 0,
                                        ymax, 0, pixelHeight,
                                    ))


                                    # Create for target raster the same projection as for the value raster
                                    raster_srs = osr.SpatialReference()
                                    raster_srs.ImportFromWkt(raster.GetProjectionRef())
                                    target_ds.SetProjection(raster_srs.ExportToWkt())


                                    # Rasterize zone polygon to raster
                                    gdal.RasterizeLayer(target_ds, [1], sub_lyr, burn_values=[1])

                                    # Read raster as arrays
                                    banddataraster = raster.GetRasterBand(1)
                                    dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

                                    bandmask = target_ds.GetRasterBand(1)
                                    datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)

                                    # Mask zone of raster and NoData values
                                    zonerast = numpy.ma.masked_array(dataraster, numpy.logical_not(datamask))
                                    nodata_value = banddataraster.GetNoDataValue()
                                    zoneraster = numpy.ma.masked_equal(zonerast, nodata_value)

                                    ID = dic_id[key + FID]
                                    print("FID " + str(FID))
                                    print("key " + str(key))
                                    print("ID " + str(ID))



                                    #Calculate statistics of zonal raster
                                    if stats == 'MEAN':
                                        nummean = numpy.mean(zoneraster)
                                        #arcpy.AddMessage("ID:" + str(ID) + "MEAN:" + str(nummean))
                                        MEAN[ID] = nummean

                                    elif stats == 'MIN':
                                        nummin = numpy.amin(zoneraster)
                                        MIN[ID] = nummin

                                    elif stats == 'MAX':
                                        nummax = numpy.amax(zoneraster)
                                        MAX[ID] = nummax
                                    elif stats == 'RANGE':
                                        numrange = numpy.ptp(zoneraster)
                                        RANGE[ID] = numrange
                                    elif stats == 'STD':
                                        numstd = numpy.std(zoneraster)
                                        STD[ID] = numstd
                                    elif stats == 'SUM':
                                        numsum = numpy.sum(zoneraster)
                                        SUM[ID] == numsum


                                except Exception as e:
                                    arcpy.AddMessage("An error occured with " + str(dic_id[FID]))
                                    pass

                        toc = time.clock()
                        tictoc = toc - tic

                else:
                    sub_lyr = lyr
                    key = 0
                    range_max = lyr_count
                    tic = time.clock()

                    for stats in stat_list:
                        for FID in range(0, range_max):
                            try:
                                ID = dic_id[key + FID]
                                feat = sub_lyr.GetFeature(FID)
                                #Get raster georeference info
                                #FID = feat.GetFID()
                                transform = raster.GetGeoTransform()
                                xOrigin = transform[0]
                                yOrigin = transform[3]
                                pixelWidth = transform[1]
                                pixelHeight = transform[5]

                                # Get extent of feat
                                geom = feat.GetGeometryRef()
                                # Fetch pointer to feature geometry
                                if (geom.GetGeometryName() == 'MULTIPOLYGON'):
                                    count = 0
                                    pointsX = [];
                                    pointsY = []

                                    for polygon in geom:
                                        #Runs through every polygon of the multipolygon feature
                                        geomInner = geom.GetGeometryRef(count)
                                        ring = geomInner.GetGeometryRef(0)
                                        numpoints = ring.GetPointCount()
                                        for p in range(numpoints):
                                            lon, lat, z = ring.GetPoint(p)
                                            pointsX.append(lon)
                                            pointsY.append(lat)
                                        count += 1
                                elif (geom.GetGeometryName() == 'POLYGON'):
                                    ring = geom.GetGeometryRef(0)
                                    numpoints = ring.GetPointCount()
                                    pointsX = []
                                    pointsY = []
                                    for p in range(numpoints):
                                        lon, lat, z = ring.GetPoint(p)
                                        pointsX.append(lon)
                                        pointsY.append(lat)

                                else:
                                    sys.exit()

                                xmin = min(pointsX)
                                xmax = max(pointsX)
                                ymin = min(pointsY)
                                ymax = max(pointsY)
                                # Specify offset and rows and columns to read
                                xoff = int((xmin - xOrigin) / pixelWidth)
                                yoff = int((yOrigin - ymax) / pixelWidth)
                                xcount = int((xmax - xmin) / pixelWidth) + 1
                                ycount = int((ymax - ymin) / pixelWidth) + 1

                                # Create memory target raster
                                target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, gdal.GDT_Byte)
                                target_ds.SetGeoTransform((
                                    xmin, pixelWidth, 0,
                                    ymax, 0, pixelHeight,
                                ))

                                # Create for target raster the same projection as for the value raster
                                raster_srs = osr.SpatialReference()
                                raster_srs.ImportFromWkt(raster.GetProjectionRef())
                                target_ds.SetProjection(raster_srs.ExportToWkt())


                                # Rasterize zone polygon to raster
                                gdal.RasterizeLayer(target_ds, [1], sub_lyr, burn_values=[1])

                                # Read raster as arrays
                                banddataraster = raster.GetRasterBand(1)
                                dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(numpy.float)

                                bandmask = target_ds.GetRasterBand(1)
                                datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(numpy.float)

                                # Mask zone of raster and NoData values
                                zonerast = numpy.ma.masked_array(dataraster, numpy.logical_not(datamask))
                                nodata_value = banddataraster.GetNoDataValue()
                                zoneraster = numpy.ma.masked_equal(zonerast, nodata_value)

                                #Calculate statistics of zonal raster
                                if stats == 'MEAN':
                                    nummean = numpy.mean(zoneraster)
                                    #arcpy.AddMessage("ID:" + str(ID) + "MEAN:" + str(nummean))
                                    MEAN[ID] = nummean

                                elif stats == 'MIN':
                                    nummin = numpy.amin(zoneraster)
                                    MIN[ID] = nummin

                                elif stats == 'MAX':
                                    nummax = numpy.amax(zoneraster)
                                    MAX[ID] = nummax
                                elif stats == 'RANGE':
                                    numrange = numpy.ptp(zoneraster)
                                    RANGE[ID] = numrange
                                elif stats == 'STD':
                                    numstd = numpy.std(zoneraster)
                                    STD[ID] = numstd
                                elif stats == 'SUM':
                                    numsum = numpy.sum(zoneraster)
                                    SUM[ID] == numsum


                            except Exception as e:
                                arcpy.AddMessage("An error occured with " + str(dic_id[FID]))
                                print e.message


                #Write the stats in a csv file
                #This code is adapted from http://stackoverflow.com/questions/22273970/writing-multiple-python-dictionaries-to-csv-file
                dicts = []
                for stat in stat_list:
                    if stat=='MEAN' :
                        dicts.append(MEAN)
                    elif stat=='RANGE':
                        dicts.append(RANGE)
                    elif stat== 'MIN':
                        dicts.append(MIN)
                    elif stat== 'MAX':
                        dicts.append(MAX)
                    elif stat== 'STD':
                        dicts.append(STD)
                    elif stat== 'SUM':
                        dicts.append(SUM)

                headers = ["ID"]
                headers.extend(stat_list)


                with open(csv_folder + '/' + buffile + '.csv','wb') as ofile:
                #The with statement allows to open the file and close it afterwards regardless of exceptions
                    writer = csv.writer(ofile, delimiter=',')
                    writer.writerow(headers)
                    for key in dicts[0].iterkeys():
                        writer.writerow([key] + [d[key] for d in dicts])
            else :
                arcpy.AddMessage("Duplicate values in ID field")
                sys.exit()
        return None

loop_zonal_stats(input_zone_polygon, input_value_raster, stat_list, buf_size, exist_buf)

if __name__ == '__main__':
    #Inputs and outputs zonal stats
    arcpy.AddMessage("Running against: {}".format(sys.version))
    if sys.maxsize > 2**32:
       arcpy.AddMessage("Running python 64 bit")
    else:
       arcpy.AddMessage("Running python 32 bit")

    # Raster dataset
    input_value_raster = arcpy.GetParameterAsText(0)
    # Vector dataset(zones)
    input_zone_polygon = arcpy.GetParameterAsText(1)
    #Buffer size
    buf_size = arcpy.GetParameterAsText(2)
    exist_buf = arcpy.GetParameterAsText(3)
    #Output csv file name
    csv_folder = arcpy.GetParameterAsText(4)
    #ID field
    ID_field = arcpy.GetParameterAsText(5)
    #Statistics
    stat = arcpy.GetParameterAsText(6)
    stat_list = stat.split(";")


    arcpy.AddMessage ("buffer size:" + str(buf_size))
    arcpy.AddMessage ("existing buffer:" + exist_buf)

    loop_zonal_stats(input_zone_polygon, input_value_raster, stat_list, buf_size, exist_buf)

    #Finish the timing of the processes
    pr.disable()
    s = StringIO.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    arcpy.AddMessage(str(s.getvalue()))
