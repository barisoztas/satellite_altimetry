import datetime
import pandas as pd
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
        for nc_file in pathlib.Path(path).glob('**/standard_measurement.nc'):
            nc_list.append(nc_file)
        return nc_list

    def read_and_calculate(self,nc_list):
        def read_df(nc):
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
            # roi_points_20 = points_20[((points_20['lat20'] < 37.74188) & (points_20['lat20'] > 37.66296) & (
            #             points_20['lon20'] < 30.13465) & (points_20['lon20'] > 30.08734))]        #burdur
            # roi_points = points[((points['lat'] < 37.74188) & (points['lat'] > 37.66296) & (points['lon'] < 30.13465) & (
            #             points['lon'] > 30.08734))]                                               #burdur
            roi_points_20 = points_20[((points_20['lat20'] < 37.75) & (points_20['lat20'] > 37.65) & (
                    points_20['lon20'] < 31.625) & (points_20['lon20'] > 31.475))]  # beysehir
            roi_points = points[((points['lat'] < 37.75) & (points['lat'] > 37.65) & (points['lon'] < 31.625) & (
                    points['lon'] > 31.475))]
            return roi_points_20, roi_points
        statistics_list = []
        counter = 0
        for nc in nc_list:
            counter = counter+1
            roi_points_20,roi_points = read_df(nc)
            water_surface_height_list = []
            for hz20_index in roi_points_20.index:
                if np.ma.is_masked(roi_points_20["alt20"][hz20_index]) | np.ma.is_masked(roi_points_20["range20"][hz20_index]):
                    continue
                else:
                    alt = roi_points_20["alt20"][hz20_index]
                    correction_list = []
                    for hz1_index in roi_points.index:
                        if np.ma.is_masked(roi_points["wet"][hz1_index]) | np.ma.is_masked(
                                roi_points["wet_model"][hz1_index]) | np.ma.is_masked(
                            roi_points["dry"][hz1_index]) | np.ma.is_masked(
                            roi_points["iono"][hz1_index]) | np.ma.is_masked(
                            roi_points["tide1"][hz1_index]) | np.ma.is_masked(roi_points["tide2"][hz1_index]):
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
            percent=counter*100/len(nc_list)
            print(f"Percent of Processed date:{percent:.2f}")
        return statistics_list

    def dict_to_excel(self, dict, path):
        df = pd.DataFrame(data=dict,index=[0])
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        df.to_excel(writer,sheet_name='results')
        writer.save()

    def run(self):
        print(f"Start time is: {self.start_time}")
        nc_list = s3_extraction.find_files(self,r"/home/hsaf/Baris/Sentinel3/s3_l2/nontimecritical/beysehir")
        statistics_list = s3_extraction.read_and_calculate(self,nc_list)
        s3_extraction.calculate_time(self)
        print(nc_list)
        print(statistics_list)


if __name__ == "__main__":
    s3_extraction_object = s3_extraction()
    s3_extraction_object.run()