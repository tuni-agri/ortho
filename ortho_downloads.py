
#############################################################################
# DOWNLOADS ORTHOIMAGES FROM (Paituli) MAANMITTAUSLAITOS to peltodata      ##
# works in peltodata, python 3.7
# After this program, run also "ortho_cut_to_reports.py"
# In peltodata, update store layers with separate script
#############################################################################


#imports
# 

import pandas as pd
import geopandas as gpd
#import matplotlib.pyplot as plt
import numpy as np
import os
import urllib 
import time
import subprocess
from osgeo import gdal

# Farms, from list
# check separate script to update the list
farms=open("/opt/oskaridata/FarmLists/farms_peltodata.txt", "r")  

#********************
# INTERSECTIONS
# identify the files, which needs to download
#********************
def def_identifyGrid(fid):
  
  # open orthogrid-file to geopandas
  grid_file='/opt/oskaridata2/ortho/OrthoAreas/OrthoAreas.shp'  # in peltodata
  # grid_file='C:/Temp/orthogridindex/OrthoAreas.shp'  # ortho grid
  orthogpd = gpd.read_file(grid_file).to_crs(epsg=3067)
  
  # open lohkorekisteri-file to geopandas
  lohkot_file='/opt/oskaridata/lohkorekisteri/plohko_cd_2019B_2_region.shp'  #  in peltodata
  #lohkot_file='C:/Temp/peltolohkorekisteri/plohko_cd_2019B_2_region.shp'
  lohkotgpd = gpd.read_file(lohkot_file).to_crs(epsg=3067)
  
  #filter lohkorekisteri to include only specific farm fields
  farmgpd = lohkotgpd[lohkotgpd.TILTU==fid]
  
  # Identify intersections, Ortho grid == farm's fields
  GridIndex, FieldIndex = farmgpd.sindex.query_bulk(orthogpd.geometry, predicate='intersects')
  
  #check
  print(GridIndex)
  print(FieldIndex)
  
  #make dataframe, where to put long path and year. This is needed in Merging whole farm png image of each year 
  #fullpathYearData = {'Longpath': [OrthoFinalFile], 'Year': [year], 'Label': [label]}
  columnNames=['Longpath','Year','Label']
  dfPathYear = pd.DataFrame(columns = columnNames) 
  
  # deletes duplicates and makes new indexing
  # https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.intersects.html#geopandas.GeoSeries.intersects
  dfgrid = pd.DataFrame(GridIndex, columns = ['GridIndex'])
  dfgrid=dfgrid.drop_duplicates(subset=['GridIndex'],ignore_index=True)
  dffield = pd.DataFrame(FieldIndex, columns = ['FieldIndex'])
  dffield=dffield.drop_duplicates(subset=['FieldIndex'],ignore_index=True) 
  
  #check
  print(dfgrid)
  print(dffield)
  
  # loop to collect all data from given farm
  for x in dfgrid.index: 
    gridindexrow=dfgrid.loc[[x],'GridIndex'].item() # get data
    # collect spesific row and columns data
    label=orthogpd.loc[[gridindexrow],'label'].item() # get data
    year=str(label[8:12]) # ortho year
    label=str(label[0:6]) # ortho label name
    path=orthogpd.loc[[gridindexrow],'path'].item() # # get data
    path=str(path[9:100]) # ortho path  
    #check
    print(year)
    print(label)
    print(path)
    
    #add data to dataframe
    OrthoFinalFile='/opt/oskaridata2/ortho/'+str(year)+'/'+label+'_'+str(year)+'.tif' #in peltodata 
    #OrthoFinalFile='C:/Temp/Ortho/'+str(year)+'/'+label+'_'+str(year)+'.tif'
    fullpathYearData = {'Longpath': OrthoFinalFile, 'Year': year, 'Label': label}
    dfPathYear=dfPathYear.append(fullpathYearData,ignore_index=True )
    
    ###sub call to download files
    def_downloadOrthos(year, label, path)
    
  return

#********************
# DOWNLOADS ORTHOS FILES
#********************

def def_downloadOrthos(year, label, path):
  
  # Paituli -service
  #  PaituliFTP = "ftp://ftp.funet.fi/index/geodata/mml/orto/"
  #  PaituliFTP = "ftp://ftp.funet.fi/index/geodata/mml/orto/"+"normal_color_3067/mavi_v_25000_50/2010/R42/10m/1/R4243C.jp2" # works
  PaituliFTP = "ftp://ftp.funet.fi/index/geodata/mml/orto/"+path
  print(PaituliFTP)
  print(path)
  
  #uploading folder for originals jp2 files from paituli
  #OrthoOrigFile='C:/Temp/Ortho/jp2/'+label+'_'+str(year)+'.jp2'
  OrthoOrigFile='/opt/oskaridata2/ortho/jp2/'+label+'_'+str(year)+'.jp2' # in peltodata
  
  #tiff files address, which has translated from jp2 to tif
  OrthoFinalFile='/opt/oskaridata2/ortho/'+str(year)+'/'+label+'_'+str(year)+'.tif'  # in peltodata
  #OrthoFinalFile='C:/Temp/Ortho/'+str(year)+'/'+label+'_'+str(year)+'.tif'
  
  #check
  print(OrthoOrigFile)
  print(OrthoFinalFile)
  
  result=None
  OrthoOrigResult = os.access(OrthoOrigFile, os.F_OK)  # check jp2 files in folder OrthoOrigFile
  OrthoTifResult = os.access(OrthoFinalFile, os.F_OK)  # check tif files in folder OrthoFinalFile
  
  # In windows, not work in Anaconda, but work with GQIS python environtment, C:\Program Files\QGIS 3.22.8\bin>
  subcall=f'gdal_translate -of GTiff -co COMPRESS=JPEG -co PHOTOMETRIC=YCBCR -a_srs EPSG:3067 -co TILED=YES -co JPEG_QUALITY=60 {OrthoOrigFile} {OrthoFinalFile}'
  
  if OrthoOrigResult == True:
    print("jp2 file has already in folder")
    #check, if tif-file has already done 
    if OrthoTifResult == True:
      print("Tif file has already in folder")
    else: 
      print("Tif file NOT in folder")
      subprocess.run(subcall, shell=True)
    return
  else:
    print("Not jp2 file")
    try:
      result=urllib.request.urlretrieve(PaituliFTP,OrthoOrigFile) # download jp2 from paituli
      time.sleep(10) #  vois olla parempikin ratkaisu...
      #check
      print(subcall)
      subprocess.run(subcall, shell=True)  
    except:
      print("error in downloading file or process jp2 to tif")
      return
  return



#*********************
# Program begins
#*******************

print("Program begins")

for x in farms:
  fid=x.rstrip() ## rstrip poistaa rivinvaihtomerkin rivin lopusta sekä mahdolliset tyhjät merkit
  print("************** FARM ",fid, "started")
  print(fid)
  def_identifyGrid(fid) #subcalls process: intersections -> downloads -> jp2ToTif
  print("************** Farm ",fid, "ready")

print('Program ready')


