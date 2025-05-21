# Main
# This is the main file for the application.

# Import necessary libraries

# System
import os

# Data
import pandas as pd
import xarray as xr
import geopandas as gpd
from shapely.geometry import mapping

# Datetime
from datetime import datetime
from datetime import timedelta

# Web
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#NoSQL - MongoDB
from pymongo import MongoClient

# local imports
import utils as ut

# Dictionary to map state abbreviations to their respective capitals
uf_br = {
          # Northern Region
          "AC": "Rio Branco",
          "AP": "Macapá",
          "AM": "Manaus",
          "PA": "Belém",
          "RO": "Porto Velho",
          "RR": "Boa Vista",
          "TO": "Palmas",
      
          # Northest Region
          "AL": "Maceió",
          "BA": "Salvador",
          "CE": "Fortaleza",
          "MA": "São Luís",
          "PB": "João Pessoa",
          "PE": "Recife",
          "PI": "Teresina",
          "RN": "Natal",
          "SE": "Aracaju",
      
          # Midwest Region
          "DF": "Brasília",
          "GO": "Goiânia",
          "MT": "Cuiabá",
          "MS": "Campo Grande",
      
          # Southeast Region
          "MG": "Belo Horizonte",
          "ES": "Vitória",
          "RJ": "Rio de Janeiro",
          "SP": "São Paulo",
      
          # South Region
          "PR": "Curitiba",
          "RS": "Porto Alegre",
          "SC": "Florianópolis"
         }

# Main
# Dates
start = datetime(2025, 1, 2, 23)  # Start date and time (UTC)
end = datetime(2025, 1, 5, 23)  # End date and time (UTC)

datasets = []

if start > end:
    print("The start date must be earlier than or equal to the end date.")
else:
    print(f"🔽 Downloading and processing data from {start} to {end}")
    dt = start
    while dt <= end:
        try:
            # 1. Download the GRIB2 file for the current hour
            ut.download_merge_cptec(dt)

            # 2. Build the file path
            filename = f"./merge_data/MERGE_CPTEC_{dt.strftime('%Y%m%d%H')}.grib2"

            # 3. Open the GRIB2 file using cfgrib
            ds = xr.open_dataset(filename, engine="cfgrib", decode_timedelta=True)
            
            # 4. Keep only the 'prec' variable
            print(ds.data_vars)
            ds = ds[["prec"]]

            # 5. Fix coordinates: convert longitude and sort lat/lon
            ds = ut.fix_coordinates(ds)

            # 6. Assign valid_time as a new dimension
            ds = ds.expand_dims("valid_time")

            # 7. Append to the list of datasets
            datasets.append(ds)

        except Exception as e:
            print(f"❌ Failed to process {dt.strftime('%Y-%m-%d %H:%M')} - {e}")

        # Advance to the next hour
        dt += timedelta(hours=1)

if datasets:
    print(f"🧩 Concatenating {len(datasets)} hourly datasets...")
    
    # 8. Concatenate all datasets along the 'valid_time' dimension
    ds_all = xr.concat(datasets, dim="valid_time")
else:
    print("⚠️ No datasets to concatenate.")

# 9. Aggregate to daily totals using 12Z-to-11Z window
ds_daily = ut.diary_prec_12z(ds_all)

# 10. Mask the data using the capitals shapefile
path_gdf = r'.\BR_Municipios_2024\BR_Municipios_2024.shp'

gdf_capitals = ut.extract_capitals_from_shapefile(path_gdf, uf_br)

# 11. Extract time series for each capital
df_br = ut.extract_capitals_timeseries(ds_daily, gdf_capitals)

# 12. Save the DataFrame to a Parquet file
# Output
output_dir = "./output"
os.makedirs(output_dir, exist_ok=True)

# Save as Parquet
df_br.to_parquet(os.path.join(output_dir, "capitals_br_daily_prec.parquet"))
print("✅ File Saved as Parquet")

# 13. Save the DataFrame.Parquet to MongoDB
ut.save_parquet_to_mongodb("./output/capitals_br_daily_prec.parquet")