import datetime
import pandas as pd
import pathlib
from netCDF4 import Dataset
import numpy as np
import os



class s3_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.input_folder = r"C:\Users\baris\OneDrive - metu.edu.tr\CE-STAR\Sentinel3\data\non time critical\beysehir"
        self.nc_list = []
        self.xlsx_list_20hz = []
        self.xlsx_list_1hz = []

    def calculate_time(self):
        self.end_time = datetime.datetime.now()
        return print(f"Total time is {self.start_time-self.end_time}")

    def find_nc_files(self):
        self.nc_list=[]
        for nc_file in pathlib.Path(self.input_folder).glob('**\\standard_measurement.nc'):
            self.nc_list.append(nc_file)
        return self.nc_list

    def find_xlsx_files(self):
        for xlsx_20 in pathlib.Path(self.input_folder).glob(('**\\*20hz.xlsx')):
            self.xlsx_list_20hz.append(xlsx_20)

        for xlsx_1 in pathlib.Path(self.input_folder).glob(('**\\*1hz.xlsx')):
            self.xlsx_list_1hz.append(xlsx_1)
        return self.xlsx_list_20hz, self.xlsx_list_1hz

    def write_to_xlsx(self):
        for nc in self.nc_list:
            roi_points_20, roi_points = s3_extraction.read_nc(nc)
            write_path = os.path.split(nc)[0]
            writer_20hz = pd.ExcelWriter(write_path+"\\output_20hz.xlsx", engine='xlsxwriter')
            writer_1hz = pd.ExcelWriter(write_path+"\\output_1hz.xlsx", engine='xlsxwriter')
            roi_points.to_excel(writer_1hz)
            writer_1hz.save()
            roi_points_20.to_excel(writer_20hz)
            writer_20hz.save()

    def read_nc(nc):
        d = Dataset(nc)
        lat = list(d.variables["lat_01"])
        lon = list(d.variables["lon_01"])
        time = list(d.variables["time_01"])
        alt = list(d.variables["alt_01"])
        wet = list(d.variables["rad_wet_tropo_cor_01_ku"])  # values are minus, so add to range
        wet_model = list(d.variables["mod_wet_tropo_cor_zero_altitude_01"])  # values are minus, so add to range
        dry = list(d.variables["mod_dry_tropo_cor_zero_altitude_01"])  # values are minus, so add to range
        tide1 = list(d.variables["solid_earth_tide_01"])  # extract from range
        tide2 = list(d.variables["pole_tide_01"])  # extract from range
        geoid = list(d.variables["geoid_01"])
        iono = list(d.variables["iono_cor_gim_01_ku"])  # values are minus, so add to range

        time20 = list(d.variables["time_20_ku"])
        lat20 = list(d.variables["lat_20_ku"])
        lon20 = list(d.variables["lon_20_ku"])
        alt20 = list(d.variables["alt_20_ku"])
        range20 = list(d.variables["range_ice_sheet_20_ku"])
        # iono20 = list(d.variables["iono_cor_alt_20_ku"])                      # values are minus, so add to range
        # #check whether it is usable for lakes or not

        #########################################################
        ##    Creating dataframes from necessary bands         ##
        #########################################################
        points_20 = pd.DataFrame({'lat20': lat20,
                                  'lon20': lon20,
                                  'time20': time20,
                                  'alt20': alt20,
                                  'range20': range20})  # ,
        # 'iono20': iono20})
        points = pd.DataFrame({'lat': lat,
                               'lon': lon,
                               'time': time,
                               'alt': alt,
                               'wet': wet,
                               'wet_model': wet_model,
                               'tide1': tide1,
                               'tide2': tide2,
                               'dry': dry,
                               'geoid': geoid,
                               'iono': iono})  # values are minus, so add to range
        #roi_points_20 = points_20[((points_20['lat20'] < 37.74188) & (points_20['lat20'] > 37.66296) & (
        #        points_20['lon20'] < 30.13465) & (points_20['lon20'] > 30.08734))]  # burdur
        #roi_points = points[((points['lat'] < 37.725) & (points['lat'] > 37.675) & (points['lon'] < 30.12) & (
        #        points['lon'] > 30.09))]  # burdur
        roi_points_20 = points_20[((points_20['lat20'] < 37.75) & (points_20['lat20'] > 37.65) & (
                points_20['lon20'] < 31.625) & (points_20['lon20'] > 31.475))]  # beysehir
        roi_points = points[((points['lat'] < 37.75) & (points['lat'] > 37.65) & (points['lon'] < 31.625) & (
                points['lon'] > 31.475))]
        return roi_points_20, roi_points

    def read_xlsx(self):
        for index in range(len(self.xlsx_list_20hz)):
            roi_points_20   = pd.read_excel(self.xlsx_list_20hz[index])
            roi_points      = pd.read_excel(self.xlsx_list_1hz[index])
            roi_points_20   =roi_points_20.set_index('Unnamed: 0')
            roi_points      =roi_points.set_index('Unnamed: 0')
        return roi_points_20, roi_points

    def read_and_calculate_from_nc(self):
        statistics_list = []
        counter = 0
        for nc in self.nc_list:
            counter = counter+1
            roi_points_20,roi_points = s3_extraction.read_nc(nc)
            water_surface_height_list = []
            for hz20_index in roi_points_20.index:
                if (roi_points_20["alt20"][hz20_index]) or (roi_points_20["range20"][hz20_index]):
                    continue
                else:
                    alt = roi_points_20["alt20"][hz20_index]
                    correction_list = []
                    for hz1_index in roi_points.index:
                        if (roi_points["wet"][hz1_index]) or (
                                roi_points["wet_model"][hz1_index]) or (
                            roi_points["dry"][hz1_index]) or (
                            roi_points["iono"][hz1_index]) or (
                            roi_points["tide1"][hz1_index]) or (roi_points["tide2"][hz1_index]):
                            continue
                        else:
                            correction = (roi_points["wet"][hz1_index]) + (roi_points["wet_model"][hz1_index]) + (
                                roi_points["dry"][hz1_index]) + (roi_points["iono"][hz1_index]) + (
                                             roi_points["tide1"][hz1_index]) + (roi_points["tide2"][hz1_index])
                            correction_list.append(correction)
                    correction = np.median(correction_list)
                    water_surface_height = alt - (roi_points_20["range20"][hz20_index] + correction) - roi_points[
                        "geoid"].median()
                water_surface_height_list.append(water_surface_height)
            wl_median = np.median(water_surface_height_list)
            wl_std = np.std(water_surface_height_list)
            new_wl_height_list = []
            for measurement in water_surface_height_list:
                difference = abs(measurement-wl_median)
                if difference >= wl_std:
                    continue
                else:
                    new_wl_height_list.append(measurement)
            new_wl_height_median = np.median(new_wl_height_list)
            new_wl_height_std = np.std(new_wl_height_list)
            new_statistics = {"Water Level": new_wl_height_median, "Uncertainity": new_wl_height_std}
            statistics_list.append(new_statistics)
            percent=counter*100/len(self.nc_list)
            print(f"Percent of Processed date:{percent:.2f}")
        return statistics_list

    def read_and_calculate_from_xlsx(self):
        statistics_list = []
        counter = 0
        for i in range(len(self.xlsx_list_1hz)):
            counter = counter+1
            roi_points_20,roi_points = s3_extraction.read_xlsx(self)
            water_surface_height_list = []
            for hz20_index in roi_points_20.index:
                if roi_points_20["alt20"][hz20_index] =='--' or roi_points_20["range20"][hz20_index] =='--':
                    continue
                else:
                    alt = roi_points_20["alt20"][hz20_index]
                    correction_list = []
                    for hz1_index in roi_points.index:
                        if (roi_points["wet"][hz1_index]) =='--' or (
                                roi_points["wet_model"][hz1_index]) =='--' or (
                            roi_points["dry"][hz1_index]) =='--' or (
                            roi_points["iono"][hz1_index])  =='--'or (
                            roi_points["tide1"][hz1_index]) =='--' or (roi_points["tide2"][hz1_index]) =='--':
                            continue
                        else:
                            correction = (roi_points["wet"][hz1_index]) + (roi_points["wet_model"][hz1_index]) + (
                                roi_points["dry"][hz1_index]) + (roi_points["iono"][hz1_index]) + (
                                             roi_points["tide1"][hz1_index]) + (roi_points["tide2"][hz1_index])
                            correction_list.append(correction)
                    correction = np.median(correction_list)
                    water_surface_height = alt - (roi_points_20["range20"][hz20_index] + correction) - roi_points[
                        "geoid"].median()
                water_surface_height_list.append(water_surface_height)
            wl_median = np.median(water_surface_height_list)
            wl_std = np.std(water_surface_height_list)
            new_wl_height_list = []
            for measurement in water_surface_height_list:
                difference = abs(measurement-wl_median)
                if difference >= wl_std:
                    continue
                else:
                    new_wl_height_list.append(measurement)
            new_wl_height_median = np.median(new_wl_height_list)
            new_wl_height_std = np.std(new_wl_height_list)
            new_statistics = {"Water Level": new_wl_height_median, "Uncertainity": new_wl_height_std}
            statistics_list.append(new_statistics)
            percent=counter*100/len(self.xlsx_list_20hz)
            print(f"Percent of Processed date:{percent:.2f}")
        return statistics_list

    def dict_to_excel(self, dict, path):
        df = pd.DataFrame(data=dict,index=[0])
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        df.to_excel(writer,sheet_name='results')
        writer.save()

    def run(self):
        print(f"Start time is: {self.start_time}")
        xlsx_list_20hz, xlsx_list_1hz = s3_extraction.find_xlsx_files(self)
        statistics_list= s3_extraction.read_and_calculate_from_xlsx(self)
        print(statistics_list)

if __name__ == "__main__":
    s3_extraction_object = s3_extraction()
    s3_extraction_object.run()