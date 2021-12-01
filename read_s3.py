import os
from netCDF4 import Dataset
import datetime
import pandas as pd
import csv


class s3_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.nc_file = r"C:\Users\baris\OneDrive - metu.edu.tr\CE-STAR\Sentinel3\data\non time " \
                       r"critical" \
                       r"\S3B_SR_2_LAN____20211028T081156_20211028T090225_20211123T013302_3029_058_292______LN3_O_NT_004.SEN3" \
                       r"\enhanced_measurement.nc"
        self.data_csv = r"C:\Users\baris\OneDrive - metu.edu.tr\CE-STAR\Sentinel3\data\non time " \
                        r"critical" \
                        r"\S3B_SR_2_LAN____20211028T081156_20211028T090225_20211123T013302_3029_058_292______LN3_O_NT_004.SEN3" \
                        r"\output"

    def reading_filtering_calculating(self):
        d = Dataset(self.nc_file)
        index = 0
        count = 0
        count2 = 0
        lat = list(d.variables["lat_01"])
        lon = list(d.variables["lon_01"])
        time = list(d.variables["time_01"])
        time20 = list(d.variables["time_20_ku"])
        lat20 = list(d.variables["lat_20_ku"])
        lon20 = list(d.variables["lon_20_ku"])

        points_20 = pd.DataFrame({'lat20': lat20,
                                  'lon20': lon20,
                                  'time20': time20})
        points = pd.DataFrame({'lat': lat,
                               'lon': lon,
                               'time': time})
        roi_points_20 = points_20[((points_20['lat20'] < 37.74188) & (points_20['lat20'] > 37.66296) & (
                    points_20['lon20'] < 30.13465) & (points_20['lon20'] > 30.08734))]
        roi_points = points[((points['lat'] < 37.74188) & (points['lat'] > 37.66296) & (points['lon'] < 30.13465) & (
                    points['lon'] > 30.08734))]

        return roi_points_20, roi_points

    def dataframe(self, points_20, points):
        points_20.to_csv(self.data_csv + '_20' + '.csv')
        points.to_csv(self.data_csv + '.csv')

    def run(self):
        print(f"Start time is: {self.start_time}")
        points_20, points = s3_extraction.reading_filtering_calculating(self)
        s3_extraction.dataframe(self, points_20, points)


if __name__ == "__main__":
    s3_extraction_object = s3_extraction()
    s3_extraction_object.run()