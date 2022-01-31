import datetime
import pandas as pd
import pathlib
from netCDF4 import Dataset
import numpy as np
import os
import dateutil.parser



class j3_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.input_folder = r"/home/hsaf/Baris/Sentinel3/Jason/jason3/beysehir"
        self.nc_list = []
        self.xlsx_list_20hz = []
        self.xlsx_list_1hz = []
        self.output_folder = r"/home/hsaf/Baris/Sentinel3/Jason/jason3/beysehir/output"

    def calculate_time(self):
        self.end_time = datetime.datetime.now()
        return print(f"Total time is {self.start_time-self.end_time}")

    def find_nc_files(self):
        self.nc_list=[]
        for nc_file in pathlib.Path(self.input_folder).glob('**/*.nc'):
            self.nc_list.append(nc_file)
        return self.nc_list

    def find_xlsx_files(self):
        for xlsx_20 in pathlib.Path(self.input_folder).glob(('**/*20hz.xlsx')):
            self.xlsx_list_20hz.append(xlsx_20)

        for xlsx_1 in pathlib.Path(self.input_folder).glob(('**/*1hz.xlsx')):
            self.xlsx_list_1hz.append(xlsx_1)
        return self.xlsx_list_20hz, self.xlsx_list_1hz

    def write_to_xlsx(self):
        counter = 0
        for nc in self.nc_list:
            counter = counter+1
            roi_points_20, roi_points = j3_extraction.read_nc(nc)
            write_path = os.path.split(nc)[0]
            writer_20hz = pd.ExcelWriter(write_path+"/output_20hz.xlsx", engine='xlsxwriter')
            writer_1hz = pd.ExcelWriter(write_path+"/output_1hz.xlsx", engine='xlsxwriter')
            roi_points.to_excel(writer_1hz)
            writer_1hz.save()
            roi_points_20.to_excel(writer_20hz)
            writer_20hz.save()
            percent = counter/len(self.nc_list)*100
            print(f"Percent of exported dates:{percent:.2f}")

    def read_nc(nc):
        d = Dataset(nc)
        roi_points_20 = pd.DataFrame({})
        roi_points = pd.DataFrame({'lat': [],
                                   'lon': [],
                                   'time': [],
                                   'alt': [],
                                   'range_ku': [],
                                   'wet': [],
                                   'rad_wet': [],
                                   'dry': [],
                                   'iono': [],
                                   'tide1': [],
                                   'tide2': [],
                                   'geoid': []})
        points_20 = pd.DataFrame({"time20": [],
                                  'lat20': [],
                                  'lon20': [],
                                  'alt20': [],
                                  'range20': []})

        try:
            lat = list(d.variables["lat"])
            lon = list(d.variables["lon"])
            time = list(d.variables["time"])
            alt = list(d.variables["alt"])
            range_ku = list(d.variables["range_ku"])
            dry = list(d.variables["model_dry_tropo_corr"])  # values are minus, so add to range
            wet = list(d.variables["model_wet_tropo_corr"])
            wet_rad = list(d.variables["rad_wet_tropo_corr"])
            iono = list(d.variables["iono_corr_alt_ku"])
            tide1 = list(d.variables["solid_earth_tide"])
            tide2 = list(d.variables["pole_tide"])
            geoid = list(d.variables["geoid"])

            time20 = list(d.variables["time_20hz"])
            lat20 = list(d.variables["lat_20hz"])
            lon20 = list(d.variables["lon_20hz"])
            alt20 = list(d.variables["alt_20hz"])
            range20 = list(d.variables["range_20hz_ku"])

            #########################################################
            ##    Creating dataframes from necessary bands         ##
            #########################################################

            points = pd.DataFrame({'lat': lat,
                                   'lon': lon,
                                   'time': time,
                                   'alt': alt,
                                   'range_ku': range_ku,
                                   'wet': wet,
                                   'rad_wet': wet_rad,
                                   'dry': dry,
                                   'iono': iono,
                                   'tide1': tide1,
                                   'tide2': tide2,
                                   'geoid': geoid})
            for i in range(len(time20)):
                lat20[i] = lat20[i].mean()
                lon20[i] = lon20[i].mean()
                time20[i] = time20[i].mean()
                range20[i] = range20[i].mean()
                alt20[i] = alt20[i].mean()

            points_20 = pd.DataFrame({"time20": time20,
                                      'lat20': lat20,
                                      'lon20': lon20,
                                      'alt20': alt20,
                                      'range20': range20})

            # roi_points_20 = points_20[(points_20['lat20'] < 37.740) & (points_20['lat20'] > 37.670)]  # burdur
            # roi_points = points[(points['lat'] < 37.740) & (points['lat'] > 37.670) ]  # burdur
            roi_points_20 = points_20[((points_20['lat20'] < 37.75) & (points_20['lat20'] > 37.65) & (
                    points_20['lon20'] < 31.625) & (points_20['lon20'] > 31.475))]  # beysehir
            roi_points = points[((points['lat'] < 37.75) & (points['lat'] > 37.65) & (points['lon'] < 31.625) & (
                    points['lon'] > 31.475))]
        except:
            pass

        return roi_points_20, roi_points

    def read_xlsx(self,path_20,path_1):
        roi_points_20   = pd.read_excel(path_20)
        roi_points      = pd.read_excel(path_1)
        roi_points_20   =roi_points_20.set_index('Unnamed: 0')
        roi_points      =roi_points.set_index('Unnamed: 0')
        return roi_points_20, roi_points


    def read_and_calculate_from_xlsx(self):
        statistics_list = []
        counter = 0
        for i in range(len(self.xlsx_list_1hz)):
            counter = counter+1
            roi_points_20,roi_points = j3_extraction.read_xlsx(self,self.xlsx_list_20hz[i], self.xlsx_list_1hz[i])
            water_surface_height_list = []
            for hz20_index in roi_points_20.index:
                if roi_points_20["alt20"][hz20_index] =='--' or roi_points_20["range20"][hz20_index] =='--':
                    continue
                else:
                    alt = roi_points_20["alt20"][hz20_index]
                    correction_list = []
                    for hz1_index in roi_points.index:
                        if (roi_points["wet"][hz1_index]) =='--' or  (
                            roi_points["dry"][hz1_index]) =='--' or (
                            roi_points["iono"][hz1_index])  =='--' or (
                            roi_points["tide1"][hz1_index]) =='--' or (roi_points["tide2"][hz1_index]) =='--' or (
                            roi_points["rad_wet"][hz1_index]) == '--':
                            continue
                        else:
                            correction = (roi_points["wet"][hz1_index]) +  (
                                roi_points["dry"][hz1_index]) + (roi_points["iono"][hz1_index])  + (
                                             roi_points["tide1"][hz1_index]) + (roi_points["tide2"][hz1_index]) + (
                                roi_points["rad_wet"][hz1_index]
                            )
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
        self.statistics_list = statistics_list
        return self.statistics_list


    def report_output(self):
        water_levels=[]
        uncertainities =[]
        dates = []
        for i in range(len(self.statistics_list)):
            water_levels.append(self.statistics_list[i]['Water Level'])
            uncertainities.append(self.statistics_list[i]['Uncertainity'])
            s3_date = ((os.path.split(os.path.split(self.xlsx_list_20hz[i])[0])[1]).split("__")[2]).split("_")[0]
            d = dateutil.parser.parse(s3_date)
            s3_date = d.strftime('%d-%m-%Y')
            dates.append(s3_date)
        result_dict = {"Date": dates,
                       "Water Surface Height Above Reference Datum":water_levels,
                       "Water Surface Height Uncertainty": uncertainities}
        results_df = pd.DataFrame.from_dict(result_dict)
        writer = pd.ExcelWriter(self.output_folder + "/Results.xlsx", engine='xlsxwriter')
        results_df.to_excel(writer,index=False)
        writer.save()

    def export_to_excel(self):
        j3_extraction.find_nc_files(self)
        j3_extraction.write_to_xlsx(self)

    def run_from_xlsx(self):
        print(f"Start time is: {self.start_time}")
        xlsx_list_20hz, xlsx_list_1hz = j3_extraction.find_xlsx_files(self)
        statistics_list= j3_extraction.read_and_calculate_from_xlsx(self)
        print(statistics_list)
        j3_extraction.report_output(self)


if __name__ == "__main__":
    j3_extraction_object = j3_extraction()
    j3_extraction_object.export_to_excel()
    j3_extraction_object.run_from_xlsx()