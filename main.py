from read_s3 import s3_extraction
import pandas as pd

a= pd.DataFrame({'a':[1]})
c= pd.DataFrame({'c':[1]})
b = s3_extraction()
b.dataframe_to_csv(a,c)