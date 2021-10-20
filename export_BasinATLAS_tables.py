from utility_functions import *

arcpy.env.overwriteOutput = True

#Directory structure
rootdir = os.path.dirname(os.path.abspath(__file__)).split('\\src')[0]
datdir = os.path.join(rootdir, 'data')
resdir = os.path.join(rootdir, 'results')
basingdb = os.path.join(datdir, 'HydroATLAS', 'BasinATLAS_v10.gdb')

#List HydroBASINS level polygon feature classes
arcpy.env.workspace = basingdb
basinfclist = arcpy.ListFeatureClasses('*')

#Create dictionary containing HYBAS_ID, basin level, and km2 for each basin in HydroBASINS
basindict = {}
for fc in basinfclist:
    lvl = re.compile("(?<=BasinATLAS_v10_)lev[0-9]{2}").search(fc).group(0)
    print(lvl)
    with arcpy.da.SearchCursor(fc, ['HYBAS_ID', 'SUB_AREA']) as cursor:
        for row in cursor:
            basindict[row[0]] = [lvl, row[1]]

basindf = pd.DataFrame.from_dict(basindict, orient= 'index').reset_index()
basindf.columns = ["HYBAS_ID", "bsn_lvl", 'SUB_AREA']
basindf.to_csv(path_or_buf=os.path.join(resdir, 'HydroBASINS_IDs.csv'))