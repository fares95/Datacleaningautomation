import pandas as pd
from AutoClean import AutoClean


df = pd.read_csv(r'C:\Users\Fares\Downloads\archives\hotel_bookings.csv')
pipeline = AutoClean(df )

output = pipeline.output
output.to_csv(r'C:\Users\Fares\Desktop\new_dataset.csv')