from read_s3 import s3_extraction
import pandas as pd

b = s3_extraction()
#b.dataframe_to_csv(a,c)
path = r"/home/hsaf/Baris/Sentinel3/s3_l2/nontimecritical/beysehir"
list = b.find_files(path)
statistics_list = b.read_and_calculate(list)
print(statistics_list)