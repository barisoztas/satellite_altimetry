import os
from netCDF4 import Dataset
import datetime

class s3_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.nc_file = r"C:\Users\baris\OneDrive - metu.edu.tr\CE-STAR\Sentinel3\data\non time " \
                       r"critical" \
                       r"\S3B_SR_2_LAN____20210904T081202_20210904T090231_20210930T003538_3029_056_292______LN3_O_NT_004.SEN3" \
                       r"\enhanced_measurement.nc"

    def reading(self):
        d = Dataset(self.nc_file)
        lat20 = list(d.variables["lat_20_ku"])
        lon20 = list(d.variables["lon_20_ku"])
        time20 = list(d.variables["time_20_ku"])
        ice20 = list(d.variables['range_ice_sheet_20_ku'])
        print(f"ice 20 length: {len(ice20)} \n time 20  len: {len(time20)}")

    def run(self):
        print(f"Start time is: {self.start_time}")
        s3_extraction.reading(self)

if __name__ == "__main__":
    s3_extraction_object = s3_extraction()
    s3_extraction_object.run()