# Functions from backend

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

# ________________________________FUNCTIONS_______________________________________

def download_merge_cptec(date: datetime, output_dir: str = "./merge_data"):
    """
    Download a MERGE_CPTEC GRIB2 file for a specific date and time.

    Parameters:
    - date (datetime): The date and hour to download (UTC).
    - output_dir (str): Directory where the file will be saved.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Format year, month, day, and hour into the URL
    url = date.strftime(
        "https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/%Y/%m/%d/MERGE_CPTEC_%Y%m%d%H.grib2"
    )
    
    filename = os.path.join(output_dir, f"MERGE_CPTEC_{date.strftime('%Y%m%d%H')}.grib2")
    
    print(f"🔄 Downloading: {url}")
    
    # Perform the download, ignoring SSL certificate verification
    response = requests.get(url, stream=True, verify=False)
    
    if response.status_code == 200:
        # Write the file in binary mode, in chunks
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Saved to: {filename}")
    else:
        print(f"❌ Failed to download {url} - Status {response.status_code}")

    print(filename)
    
    return filename


def fix_coordinates(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert longitude coordinates from [0, 360] to [-180, 180]
    and ensure both latitude and longitude are sorted in ascending order.

    Parameters:
    - ds (xr.Dataset): Input dataset with 'latitude' and 'longitude' dimensions.

    Returns:
    - xr.Dataset: Dataset with adjusted and sorted coordinates.
    """
    # Convert longitude to range [-180, 180] if necessary
    lon = ds.longitude
    if (lon > 180).any():
        ds = ds.assign_coords(longitude=(((lon + 180) % 360) - 180))

    # Sort longitudes in ascending order
    ds = ds.sortby("longitude")

    # Sort latitudes in ascending order
    ds = ds.sortby("latitude")

    return ds


def diary_prec_12z(ds: xr.Dataset) -> xr.Dataset:
    """
    Aggregate hourly precipitation data into daily totals, using a 12Z-to-11Z window.

    Parameters:
    - ds (xr.Dataset): Dataset containing hourly precipitation with 'valid_time' as time coordinate.

    Returns:
    - xr.Dataset: Daily aggregated dataset with 'ref_time' indicating the 12Z start of each accumulation window.
    """
    ds = ds.copy()

    # Define the start of the 12Z–11Z accumulation window
    ref_time = (ds.valid_time - pd.Timedelta(hours=12)).dt.floor("1D") + pd.Timedelta(hours=12)
    ds.coords["ref_time"] = ref_time

    # Group by ref_time and sum the precipitation
    ds_daily = ds.groupby("ref_time").sum()

    ds_daily.attrs["aggregation_period"] = "From 12Z to 11Z (ref_time = 12Z start of period)"
    
    return ds_daily


def extract_capitals_from_shapefile(shapefile_path: str, capitals_dict: dict) -> gpd.GeoDataFrame:
    """
    Extracts the capital cities from a shapefile of Brazilian municipalities using a UF-to-capital dictionary.

    Parameters:
    - shapefile_path (str): Path to the shapefile containing Brazilian municipalities.
    - capitals_dict (dict): Dictionary mapping UF abbreviations (e.g., 'SP') to capital city names (e.g., 'São Paulo').

    Returns:
    - GeoDataFrame: Filtered GeoDataFrame containing only the capital cities.
    """
    # Load the shapefile
    gdf = gpd.read_file(shapefile_path)

    # Filter rows where UF and city name match the dictionary
    gdf_capitals = gdf[gdf.apply(
        lambda row: row['SIGLA_UF'] in capitals_dict and row['NM_MUN'] == capitals_dict[row['SIGLA_UF']],
        axis=1
    )].copy()

    gdf_capitals.reset_index(drop=True, inplace=True)

    return gdf_capitals


def mask_data(ds: xr.Dataset, mask: str) -> xr.Dataset:
    """
    Mask data within a geographical extension directly from a shapefile.

    Parameters
    ----------
        ds : ``xarray.Dataset``
            Two-dimensional (2D) array database to which to apply mask.

        mask : str, path object or file-like object
            Shapefile to mask dataset.

    Returns
    -------
        ``xarray.Dataset``
            Masked data.
    """
    if isinstance(mask, gpd.GeoDataFrame) is True:
        # Do not read from shapefile
        geo_df = mask
    else:
        # Read from a shapefile
        geo_df = gpd.read_file(mask)

    # Set the spatial dimensions of the dataset
    ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
    
    # Write the CRS to the dataset in a CF compliant manner
    ds.rio.write_crs("epsg:4326", inplace=True)

    # Clip using a GeoDataFrame
    return ds.rio.clip(geo_df.geometry.apply(mapping), geo_df.crs, drop=False)


def extract_capitals_timeseries(ds_daily: xr.Dataset, gdf_capitals: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Extracts the mean daily precipitation time series for each Brazilian capital.

    Parameters:
    - ds_daily (xr.Dataset): Daily gridded dataset with 'latitude' and 'longitude' dimensions.
    - gdf_capitals (gpd.GeoDataFrame): GeoDataFrame with capital geometries.

    Returns:
    - pd.DataFrame: Wide-format DataFrame with 'ref_time' as index and one column per capital.
    """
    df_br_list = []

    for capital in gdf_capitals['NM_MUN']:
        print(f"🔍 Masking data for {capital}...")

        mask = gdf_capitals[gdf_capitals["NM_MUN"] == capital]
        ds_capital = mask_data(ds_daily, mask)

        # Mean over spatial dimensions
        capital_ds_mean = ds_capital.mean(dim=['latitude', 'longitude'])

        # Convert to DataFrame
        df_capital = capital_ds_mean.to_dataframe().dropna()

        # Drop unnecessary columns
        df_capital = df_capital.drop(columns=[col for col in ['spatial_ref', 'step', 'surface'] if col in df_capital.columns])

        # Rename 'prec' to capital name
        df_capital = df_capital.rename(columns={"prec": capital})

        # Append
        df_br_list.append(df_capital[[capital]])  # Keep only the renamed column

    # Concatenate on columns (axis=1) using ref_time as index
    df_br = pd.concat(df_br_list, axis=1)

    return df_br


def save_parquet_to_mongodb(parquet_path: str,
                            db_name: str = "capitals",
                            collection_name: str = "precipitacao_diaria") -> None:
    """
    Reads a Parquet file and saves its content to a MongoDB collection.

    Parameters:
    - parquet_path (str): Path to the Parquet file to be inserted.
    - db_name (str): Name of the MongoDB database (default is 'clima').
    - collection_name (str): Name of the MongoDB collection (default is 'precipitacao_diaria').

    Returns:
    - None
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"File not found: {parquet_path}")

    print(f"📂 Reading Parquet file: {parquet_path}")
    df = pd.read_parquet(parquet_path)

    # Ensure temporal index is preserved
    df = df.reset_index()

    print(f"🔄 Converting DataFrame to JSON documents...")
    data_dict = df.to_dict(orient="records")

    print(f"🔌 Connecting to MongoDB...")
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]

    print(f"📝 Inserting {len(data_dict)} records into '{db_name}.{collection_name}'...")
    collection.insert_many(data_dict)

    print("✅ Data successfully saved to MongoDB.")