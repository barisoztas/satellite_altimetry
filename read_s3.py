import datetime
import pandas as pd
import glob
import pathlib
from netCDF4 import Dataset
import numpy as np



class s3_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None

    def calculate_time(self):
        self.end_time = datetime.datetime.now()
        return print(f"Total time is {self.start_time-self.end_time}")

    def find_files(self,path):
        nc_list =[]
        for nc_file in pathlib.Path(path).glob('**\standard_measurement.nc'):
            nc_list.append(nc_file)
        return nc_list

    def read_and_calculate(self,nc_list):
        global roi_points
        for nc in nc_list:
            d = Dataset(nc)
            lat = list(d.variables["lat_01"])
            lon = list(d.variables["lon_01"])
            time = list(d.variables["time_01"])
            alt = list(d.variables["alt_01"])
            wet = list(d.variables["rad_wet_tropo_cor_01_ku"])                   # values are minus, so add to range
            wet_model = list(d.variables["mod_wet_tropo_cor_zero_altitude_01"])  # values are minus, so add to range
            dry = list(d.variables["mod_dry_tropo_cor_zero_altitude_01"])        # values are minus, so add to range
            tide1 = list(d.variables["solid_earth_tide_01"])                     # extract from range
            tide2 = list(d.variables["pole_tide_01"])                            # extract from range
            geoid = list(d.variables["geoid_01"])
            iono = list(d.variables["iono_cor_gim_01_ku"])                       # values are minus, so add to range

            time20 = list(d.variables["time_20_ku"])
            lat20 = list(d.variables["lat_20_ku"])
            lon20 = list(d.variables["lon_20_ku"])
            alt20 = list(d.variables["alt_20_ku"])
            range20 = list(d.variables["range_ice_sheet_20_ku"])
            # iono20 = list(d.variables["iono_cor_alt_20_ku"])                      # values are minus, so add to range #check whether it is usable for lakes or not

            #########################################################
            ##    Creating dataframes from necessary bands         ##
            #########################################################
            points_20 = pd.DataFrame({'lat20': lat20,
                                    'lon20': lon20,
                                    'time20': time20,
                                    'alt20': alt20,
                                    'range20': range20}) #,
                                    #'iono20': iono20})
            points = pd.DataFrame({'lat' : lat,
                                    'lon' : lon,
                                    'time' : time,
                                    'alt' : alt,
                                    'wet' : wet,
                                    'wet_model' : wet_model,
                                    'tide1': tide1,
                                    'tide2': tide2,
                                    'dry' : dry,
                                   'geoid':geoid,
                                   'iono' : iono})                      # values are minus, so add to range
            # roi_points_20 = points_20[((points_20['lat20'] < 37.74188) & (points_20['lat20'] > 37.66296) & (
            #             points_20['lon20'] < 30.13465) & (points_20['lon20'] > 30.08734))]        #burdur
            # roi_points = points[((points['lat'] < 37.74188) & (points['lat'] > 37.66296) & (points['lon'] < 30.13465) & (
            #             points['lon'] > 30.08734))]                                               #burdur
            roi_points_20 = points_20[((points_20['lat20'] < 37.75) & (points_20['lat20'] > 37.65) & (
                        points_20['lon20'] < 31.625) & (points_20['lon20'] > 31.475))]          #beysehir
            roi_points = points[((points['lat'] < 37.75) & (points['lat'] > 37.65) & (points['lon'] < 31.625) & (
                        points['lon'] > 31.475))]                                                 #beysehir
            water_surface_height_list = []
            for index in roi_points_20.index:
                alt = roi_points_20["alt20"][index]
                corrected_range =  roi_points_20["range20"][index] + np.median(roi_points["wet"]) + \
                                   np.median(roi_points["wet_model"]) + np.median(roi_points["dry"]) + \
                                   np.median(roi_points["iono"]) - np.median(roi_points["tide1"]) - \
                                    np.median(roi_points["tide2"])
                water_surface_height = alt - corrected_range - np.median(roi_points["geoid"])
                water_surface_height_list.append(water_surface_height)
            print(water_surface_height_list)
            water_surface_height_median = np.nanmedian(water_surface_height_list)
            water_surface_height_std = np.nanstd(water_surface_height_list)
            statistics = {"Water Level":water_surface_height_median,
                          "Water Level Standard Deviation": water_surface_height_std}
            return statistics



    def dataframe_to_excel(self, points_20, points):
        writer = pd.ExcelWriter(self.data_csv, engine='xlsxwriter')
        points_20.to_excel(writer,sheet_name='20hz')
        points.to_excel(writer, sheet_name='1hz')
        writer.save()

    def run(self):
        print(f"Start time is: {self.start_time}")
        points_20, points = s3_extraction.read_and_calculate(self)
        s3_extraction.dataframe_to_excel(self, points_20, points)


if __name__ == "__main__":
    s3_extraction_object = s3_extraction()
    s3_extraction_object.run()