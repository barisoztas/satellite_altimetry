from netCDF4 import Dataset
import datetime
import pandas as pd


class s3_extraction(object):
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.nc_file = r"C:\Users\baris\OneDrive - metu.edu.tr\CE-STAR\Sentinel3\data\non time " \
                       r"critical\beysehir" \
                       r"\S3A_SR_2_LAN____20211022T080629_20211022T085657_20211116T234524_3028_077_349______LN3_O_NT_004" \
                       r"\enhanced_measurement.nc"
        self.data_csv = r"C:\Users\baris\OneDrive - metu.edu.tr\CE-STAR\Sentinel3\data\non time " \
                        r"critical\output" \
                        r"\output"

    def reading_filtering(self):
        d = Dataset(self.nc_file)
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
        iono = list(d.variables["iono_cor_alt_01_ku"])                       # values are minus, so add to range

        time20 = list(d.variables["time_20_ku"])
        lat20 = list(d.variables["lat_20_ku"])
        lon20 = list(d.variables["lon_20_ku"])
        alt20 = list(d.variables["alt_20_ku"])
        range20 = list(d.variables["range_ice_sheet_20_ku"])
        iono20 = list(d.variables["iono_cor_alt_20_ku"])                      # values are minus, so add to range


        #########################################################
        ##    Creating dataframes from necessary bands         ##
        #########################################################
        points_20 = pd.DataFrame({'lat20': lat20,
                                'lon20': lon20,
                                'time20': time20,
                                'alt_20': alt20,
                                'range20': range20,
                                'iono20': iono20})
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
                               'iono:' : iono})                      # values are minus, so add to range
        # roi_points_20 = points_20[((points_20['lat20'] < 37.74188) & (points_20['lat20'] > 37.66296) & (
        #             points_20['lon20'] < 30.13465) & (points_20['lon20'] > 30.08734))]        #burdur
        # roi_points = points[((points['lat'] < 37.74188) & (points['lat'] > 37.66296) & (points['lon'] < 30.13465) & (
        #             points['lon'] > 30.08734))]                                               #burdur
        roi_points_20 = points_20[((points_20['lat20'] < 37.75) & (points_20['lat20'] > 37.67) & (
                    points_20['lon20'] < 31.61) & (points_20['lon20'] > 31.49))]          #beysehir
        roi_points = points[((points['lat'] < 37.75) & (points['lat'] > 37.67) & (points['lon'] < 31.61) & (
                    points['lon'] > 31.49))]                                                 #beysehir


        return roi_points_20, roi_points

    def dataframe(self, points_20, points):
        points_20.to_csv(self.data_csv + '_20' + '.csv')
        points.to_csv(self.data_csv + '.csv')

    def run(self):
        print(f"Start time is: {self.start_time}")
        points_20, points = s3_extraction.reading_filtering(self)
        s3_extraction.dataframe(self, points_20, points)


if __name__ == "__main__":
    s3_extraction_object = s3_extraction()
    s3_extraction_object.run()