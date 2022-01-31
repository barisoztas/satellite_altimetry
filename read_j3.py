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
        self.input_folder = r"/home/baris/altimetry/jason/jason3/beysehir/input"
        self.nc_list = []
        self.xlsx_list_20hz = []
        self.xlsx_list_1hz = []
        self.output_folder = r"/home/baris/altimetry/jason/jason3/beysehir/output"

    def calculate_time(self):
        self.end_time = datetime.datetime.now()
        return print(f"Total time is {self.start_time - self.end_time}")

    def find_nc_files(self):
        self.nc_list = []
        for nc_file in pathlib.Path(self.input_folder).glob('**/*.nc'):
            self.nc_list.append(nc_file)
        return self.nc_list

    def find_xlsx_files(self):
        for xlsx_20 in pathlib.Path(self.input_folder).glob(('**/*20hz.xlsx')):
            self.xlsx_list_20hz.append(xlsx_20)

        for xlsx_1 in pathlib.Path(self.input_folder).glob(('**/*1hz.xlsx')):
            self.xlsx_list_1hz.append(xlsx_1)
        return self.xlsx_list_20hz, self.xlsx_list_1hz

    def read_nc(nc):
        d = Dataset(nc)
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
        roi_points_20 = pd.DataFrame({"time20": [],
                                      'lat20': [],
                                      'lon20': [],
                                      'alt20': [],
                                      'range20': []})

        if d.title != 'GDR - Standard dataset' or len(d.variables)==0:      # omit reduced datasets = no variable available
            return roi_points_20, roi_points

        # read 1hz measurement
        lat = list(d.variables["lat"])
        lon = list(d.variables["lon"])
        time = list(d.variables["time"])
        alt = list(d.variables["alt"])
        range_ku = list(d.variables["range_ku"])
        dry = list(d.variables["model_dry_tropo_corr"])  # values are minus, so add to range
        #wet = list(d.variables["model_wet_tropo_corr"])
        wet_rad = list(d.variables["rad_wet_tropo_corr"])
        iono = list(d.variables["iono_corr_alt_ku"])
        #iono_gim_model = list(d.variables["iono_corr_gim_ku"])
        tide1 = list(d.variables["solid_earth_tide"])
        tide2 = list(d.variables["pole_tide"])
        geoid = list(d.variables["geoid"])
        # read 20hz measurement
        time20_pack = list(d.variables["time_20hz"])
        lat20_pack = list(d.variables["lat_20hz"])
        lon20_pack = list(d.variables["lon_20hz"])
        alt20_pack = list(d.variables["alt_20hz"])
        range20_pack = list(d.variables["range_20hz_ku"])

        #########################################################
        ##    Creating dataframes from necessary bands         ##
        #########################################################

        roi_points = pd.DataFrame({'lat': lat,
                                   'lon': lon,
                                   'time': time,
                                   'alt': alt,
                                   'range_ku': range_ku,
                                   'rad_wet': wet_rad,
                                   'dry': dry,
                                   'iono': iono,
                                   'tide1': tide1,
                                   'tide2': tide2,
                                   'geoid': geoid})

        lat20 = []
        lon20 = []
        time20 = []
        range20 = []
        alt20 = []

        # convert them to list type
        for i in range(len(time20_pack)):
            lat20.append(lat20_pack[i].tolist())
            lon20.append(lon20_pack[i].tolist())
            time20.append(time20_pack[i].tolist())
            range20.append(range20_pack[i].tolist())
            alt20.append(alt20_pack[i].tolist())

        lat20 = sum(lat20, [])
        lon20 = sum(lon20, [])
        time20 = sum(time20, [])
        range20 = sum(range20, [])
        alt20 = sum(alt20, [])

        roi_points_20 = pd.DataFrame({"time20": time20,
                                      'lat20': lat20,
                                      'lon20': lon20,
                                      'alt20': alt20,
                                      'range20': range20})

        # roi_points_20 = points_20[(points_20['lat20'] < 37.740) & (points_20['lat20'] > 37.670)]  # burdur
        # roi_points = points[(points['lat'] < 37.740) & (points['lat'] > 37.670) ]  # burdur
        roi_points_20 = roi_points_20[((roi_points_20['lat20'] < 37.75) & (roi_points_20['lat20'] > 37.65) & (
                roi_points_20['lon20'] < 31.625) & (roi_points_20['lon20'] > 31.475))]  # beysehir
        roi_points = roi_points[
            ((roi_points['lat'] < 37.75) & (roi_points['lat'] > 37.65) & (roi_points['lon'] < 31.625) & (
                    roi_points['lon'] > 31.475))]

        return roi_points_20, roi_points

    def write_to_xlsx(self):
        counter = 0
        for nc in self.nc_list:
            counter = counter + 1
            roi_points_20, roi_points = j3_extraction.read_nc(nc)
            write_path = os.path.split(nc)[0]
            writer_20hz = pd.ExcelWriter(write_path + "/output_20hz.xlsx", engine='xlsxwriter')
            writer_1hz = pd.ExcelWriter(write_path + "/output_1hz.xlsx", engine='xlsxwriter')
            roi_points.to_excel(writer_1hz)
            writer_1hz.save()
            roi_points_20.to_excel(writer_20hz)
            writer_20hz.save()
            percent = counter / len(self.nc_list) * 100
            print(f"Percent of exported dates:{percent:.2f}")

    def read_xlsx(self, path_20, path_1):
        roi_points_20 = pd.read_excel(path_20)
        roi_points = pd.read_excel(path_1)
        roi_points_20 = roi_points_20.set_index('Unnamed: 0')
        roi_points = roi_points.set_index('Unnamed: 0')
        return roi_points_20, roi_points

    def read_and_calculate_from_xlsx(self):
        statistics_list = []
        counter = 0
        for i in range(len(self.xlsx_list_1hz)):
            counter = counter + 1
            roi_points_20, roi_points = j3_extraction.read_xlsx(self, self.xlsx_list_20hz[i], self.xlsx_list_1hz[i])
            water_surface_height_list = []
            for hz20_index in roi_points_20.index:
                if roi_points_20["alt20"][hz20_index] == '--' or roi_points_20["range20"][hz20_index] == '--':
                    continue
                else:
                    alt = roi_points_20["alt20"][hz20_index]
                    correction_list = []
                    for hz1_index in roi_points.index:
                        if (roi_points["wet"][hz1_index]) == '--' or (
                                roi_points["dry"][hz1_index]) == '--' or (
                                roi_points["iono"][hz1_index]) == '--' or (
                                roi_points["tide1"][hz1_index]) == '--' or (roi_points["tide2"][hz1_index]) == '--' or (
                                roi_points["rad_wet"][hz1_index]) == '--':
                            continue
                        else:
                            correction = (roi_points["wet"][hz1_index]) + (
                                roi_points["dry"][hz1_index]) + (roi_points["iono"][hz1_index]) + (
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
                difference = abs(measurement - wl_median)
                if difference >= wl_std:
                    continue
                else:
                    new_wl_height_list.append(measurement)
            new_wl_height_median = np.median(new_wl_height_list)
            new_wl_height_std = np.std(new_wl_height_list)
            new_statistics = {"Water Level": new_wl_height_median, "Uncertainity": new_wl_height_std}
            statistics_list.append(new_statistics)
            percent = counter * 100 / len(self.xlsx_list_20hz)
            print(f"Percent of Processed date:{percent:.2f}")
        self.statistics_list = statistics_list
        return self.statistics_list

    def report_output(self):
        water_levels = []
        uncertainities = []
        dates = []
        for i in range(len(self.statistics_list)):
            water_levels.append(self.statistics_list[i]['Water Level'])
            uncertainities.append(self.statistics_list[i]['Uncertainity'])
            j3_date = ((os.path.split(os.path.split(self.xlsx_list_20hz[i])[0])[1]).split("_")[4])
            d = dateutil.parser.parse(j3_date)
            s3_date = d.strftime('%d-%m-%Y')
            dates.append(s3_date)
        result_dict = {"Date": dates,
                       "Water Surface Height Above Reference Datum": water_levels,
                       "Water Surface Height Uncertainty": uncertainities}
        results_df = pd.DataFrame.from_dict(result_dict)
        writer = pd.ExcelWriter(self.output_folder + "/Results.xlsx", engine='xlsxwriter')
        results_df.to_excel(writer, index=False)
        writer.save()

    def export_to_excel(self):
        j3_extraction.find_nc_files(self)
        j3_extraction.write_to_xlsx(self)

    def run_from_xlsx(self):
        print(f"Start time is: {self.start_time}")
        j3_extraction.find_xlsx_files(self)
        statistics_list = j3_extraction.read_and_calculate_from_xlsx(self)
        print(statistics_list)
        j3_extraction.report_output(self)


if __name__ == "__main__":
    j3_extraction_object = j3_extraction()
    #j3_extraction_object.export_to_excel()
    j3_extraction_object.run_from_xlsx()