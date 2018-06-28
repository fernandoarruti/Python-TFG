# coding=utf8
import pandas as pd
import numpy as np
import xarray as xr
from glob import glob
import os


def read_ifile(ifile):
    data=pd.read_csv(ifile,encoding="iso-8859-1",sep=";",parse_dates=[["AÑO","MES","DIA"]],index_col=0,dtype='unicode')

    return data


varname2filename = {
    "pr": "P77",
    "tasmax": "TMAX",
    "tasmin": "TMIN",
    "tasmed": "TMED",
    }

cf2attrs = {
    "pr": {
        "long_name": "Accumulated precipitation",
        "standard_name": "precipitation_amount",
        "units": "kg m-2"
    },
    "tasmax": {
        "long_name": "Daily Maximum Near-Surface Air Temperature",
        "standard_name": "air_temperature",
        "units" : "Celsius"
    },
    "tasmin": {
        "long_name": "Daily Minimum Near-Surface Air Temperature",
        "standard_name": "air_temperature",
        "units" : "Celsius"
    },
    "tasmed": {
        "long_name": "Daily Mean Near-Surface Air Temperature",
        "standard_name": "air_temperature",
        "units" : "Celsius"
    },
    "lon": {
        "long_name" : "Longitude",
        "standard_name" : "longitude",
        "units" : "degrees_east"
    },

    "lat": {
        "long_name" : "Latitude",
        "standard_name" : "latitude",
        "units" : "degrees_north"
    },

    "alt": {
        "long_name" : "Vertical distance above the surface",
        "standard_name" : "height",
        "units" : "m",
        "axis" : "Z",
        "positive": "up"
    },
    
    "time": {
        "long_name" : "time",
        "standard_name" : "time"
    },

    "station" : {
        "long_name" : "station name",
        "cf_role" : "timeseries_id"
    },
    
    "name" : {
        "long_name" : "Station Description",
        "standard_name" : "station_description",
        "units" : "name of the station",
        
    }
}
def process_filedata(file_data,varname):
    
    ####filtrar los datos y obtener solo los importantes(INDICATIVO Y DATO)
    
    var_name_file=varname2filename[varname]
    file_data=file_data.filter(items=['INDICATIVO',var_name_file])
    ###Reemplazar los -3 (inapreciable) por 0
    file_data[var_name_file].replace('-3', 0,inplace=True)
    #Reemplazar los -4 y su siguiente por nan
    file_data.loc[file_data[var_name_file].shift(1) == '-4', var_name_file] = np.nan
    
   
    file_data[var_name_file].replace('-4', np.nan,inplace=True)
    print(file_data[var_name_file])
    #renombrar nombre variables al estandar, indice como Time e indicativo como station
    file_data.rename(columns={var_name_file: varname, 'INDICATIVO': 'station'}, inplace=True)
    file_data.index.name = 'time'
    #multiplicar las unidades por 0,1
    file_data[varname] = file_data[varname].astype(float)
    ##print (data.dtypes)
    file_data[varname] = file_data[varname]*0.1
    
    
    #asigno el multiindex
    file_data_index_resetted = file_data.reset_index()
    file_data_mindex = file_data_index_resetted.set_index(["station", "time"])
    ###reconvertir en dataframe
    file_data=file_data_mindex.unstack()[varname].T
    
   
    if not file_data.index.is_unique:
        file_data = file_data.loc[~file_data.index.duplicated()]
    
    #Detectar fechas ausentes. Si hay, rellenar con nan asfrec
    file_data = file_data.asfreq('D')
    #idx = pd.date_range('01-01-1950', '31-12-1954')
    #file_data =file_data.reindex(idx, fill_value=np.nan)
    
    #transformar el dataframe en un xarray dataset
    #necesaria instalacion de xarray
    file_data=file_data.stack().to_xarray()
    return file_data
    

def get_folder_filelist(varname, folder, idir): ##funciona
    if(varname is "pr"):
        varname="Datos-Pd"
        
    
    if(varname is "tasmax" or varname is "tasmin" or varname is "tasmed"):
        varname="Datos-Td"
    
        
    path_to_expand = "{}/{}/{}*.txt".format(idir, folder, varname)
   
    filelist = glob(path_to_expand)
    return filelist


def process_folder_data(varname, folder, idir):
    filelist = get_folder_filelist(varname, folder, idir)#obtengo los ficheros en el directorio
    file_data_list = []
    print("ENTRA A IMPRIMIR FILEDATALIST")
    for ifile in filelist:
        file_data = read_ifile(ifile)
        file_data_processed = process_filedata(file_data, varname)#obtengo los valores requeridos en el fichero
        file_data_list.append(file_data_processed)

    vardata = xr.concat(file_data_list, dim="time")
    duplicates_mask = ~vardata.time.to_index().duplicated()
    vardata = vardata.sel(time=duplicates_mask)
    return vardata


def add_locations(vardata):
    dataExcel = pd.read_excel(open(r'C:\Users\ferna\Desktop\RED-SECUNDARIA\Peticion390160095\Estaciones.xls','rb'), sheetname='Sheet1',index_col=0)
    stations = vardata.station.to_index()
    dataExcel = dataExcel.loc[stations]
    dataExcel.index.name = "station"
    vardata["lon"] = dataExcel["LON"].to_xarray()
    vardata["lat"] = dataExcel["LAT"].to_xarray()
    vardata["alt"] = dataExcel["ALT"].to_xarray()
    vardata["name"] = dataExcel["ESTACION"].to_xarray()
    vardata["station"].attrs = cf2attrs["station"]
    
    return vardata


def add_attributes(vardata, varname):
    vardata.data_vars[varname].attrs = cf2attrs[varname]
    vardata["lon"].attrs = cf2attrs["lon"]
    vardata["lat"].attrs = cf2attrs["lat"]
    vardata["alt"].attrs = cf2attrs["alt"]
    vardata["station"].attrs = cf2attrs["station"]
    vardata["name"].attrs = cf2attrs["name"]
    vardata.attrs = {"featureType": "timeSeries", "Conventions": "CF-1.4"}
    print(vardata.values())
    return vardata

def process_variable_data(varname):
    folder_list = [d for d in os.listdir('C:/Users/ferna/Desktop/RED-SECUNDARIA/Peticion390160095') if os.path.isdir(os.path.join('C:/Users/ferna/Desktop/RED-SECUNDARIA/Peticion390160095', d))]
    
    folder_data_list = []
    idir="C:/Users/ferna/Desktop/RED-SECUNDARIA/Peticion390160095"
    for folder in folder_list:
        print("ENTRA A CARPETA-------------------")
        print(folder)
        folder_data = process_folder_data(varname, folder,idir)
       
        if not folder_data.time.to_index().is_unique:
            print("Data of variable {} in folder {} as a not unique time axis".format(folder, varname))
       
        folder_data_list.append(folder_data)
        
        print(folder_data_list)

    print("SALE")
    print(folder_data_list)
    print("SALE----------------------------------")
    
    
    vardata = xr.concat(folder_data_list, dim="station")
    vardata=vardata.to_dataset(name=varname)#CAMBIOS       
    odir = "C:/Users/ferna/Desktop/netcdfFiles"
    vardatafile = "{}/{}_red_secundaria_aemet.nc".format(odir, varname)
    print(vardatafile)
    vardata = add_locations(vardata)
    vardata = add_attributes(vardata, varname)#ANADO E IMPRIMO ATBOS
    print("ENTRA A IMPRIMIR EL DATASET FINAL")
    print(vardata)
    vardata = vardata.set_coords(["lon", "lat", "alt","name"]).transpose()#anadir y ordenar las coordenadas
    vardata.to_netcdf(vardatafile)
	

def main():
    
    for varname in varname2filename: 
        print("Entrada variable: "+varname)
        process_variable_data(varname)
    
    
    
main()
