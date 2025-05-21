# ☁️ Brazilian Capital Cities - Daily Precipitation ETL (MERGE/CPTEC)

This project is corresponds in a ETL pipeline that downloads **hourly precipitation data** from the [MERGE/CPTEC](https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/) repository, aggregates it into **daily rainfall totals (12Z–12Z)**, extracts precipitation for **all Brazilian capital cities**, and stores the data in **Apache Parquet format** and a **MongoDB database**.

---

## 1. Environment Setup

This project uses **Conda** virtual environment. You can install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://www.anaconda.com/).

To create the environment:

```bash
conda env create -f environment.yml
conda activate chuva_merge
```

---

## 2. Running the ETL Pipeline

The main script is `app.py`, which downloads and processes data for a user-defined period.

### Select the Period

Inside `app.py`, you can configure the data range to be downloaded by modifying:

```python
start = datetime(2025, 1, 2, 23)  # Start datetime (UTC)  ## Format is (YYYY, M, D, HH)
end   = datetime(2025, 1, 5, 23)  # End datetime (UTC)  ## Format is (YYYY, M, D, HH)
```

Use **UTC time** and ensure `start <= end`.

### To Run:

```bash

conda activate chuva_merge

python app.py

```

---

## 3. Outputs

1. A `.parquet` file will be saved to the `./output/` folder:
   ```
   capitals_br_daily_prec.parquet
   ```
2. The same data will be **uploaded to MongoDB** under:
   - Database: `capitals`
   - Collection: `precipitacao_diaria`

---

## 4. Why Apache Parquet?

Apache Parquet is a modern columnar storage format specifically designed for large-scale analytical workloads. Compared to traditional formats such as CSV, Parquet offers several distinct advantages. Notably, it supports built-in compression, reducing file size and improving storage efficiency. Unlike CSV, which lacks schema definitions, Parquet maintains a well-defined schema, facilitating data consistency and interoperability. It also provides superior performance, particularly in read operations, and allows for the selective retrieval of columns, which optimizes processing time and resource usage. These features make Parquet particularly well-suited for time series and geospatial data, where high-performance access and compact storage are critical requirements.

---

## 5. Why MongoDB?

MongoDB is a document-oriented NoSQL database that offers significant advantages for handling complex and heterogeneous data structures. Its flexible schema design allows for the seamless management of geospatial and time-indexed data without the constraints of predefined columns, which is particularly beneficial for environmental datasets. MongoDB is inherently scalable, supporting distributed architectures and high availability. Moreover, it provides efficient storage capabilities, especially when dealing with nested or hierarchical data structures, such as those often encountered in climate research.
In comparison to traditional relational databases like PostgreSQL or MySQL, MongoDB does not require a fixed schema, making it more adaptable to evolving data formats. While relational databases can support geospatial queries through extensions like PostGIS, MongoDB includes native geospatial functionalities. Furthermore, whereas relational databases offer limited support for JSON, MongoDB stores data in BSON, enabling full support for hierarchical and embedded documents. These characteristics make MongoDB particularly well-suited for applications involving climate and environmental data.

---

## 6. Project Structure

```
 app.py                 # Main ETL pipeline script
 utils.py              # Helper functions for download, mask, extraction, and DB
 environment.yml       # Conda environment
 output/               # Parquet output folder
 merge_data/           # Folder for downloaded GRIB2 files
 BR_Municipios_2024/   # Shapefile folder with Brazilian municipalities
```

---

## 7. Geographic Data

The shapefile of Brazilian municipalities can be found at: https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html

It must be downloaded and placed in the same directory as the other project files, specifically within the BR_Municipios_2024 folder.

Thus, the correct path to reference the file is: ./BR_Municipios_2024/BR_Municipios_2024.shp

---

## 8. Example Output

Once the pipeline finishes, you will have a Parquet file like:

```
ref_time             São Paulo  Rio de Janeiro  Brasília  ...
2025-01-03 12:00:00     5.6          3.1          0.0
2025-01-04 12:00:00     8.9          1.5          1.2
...
```

And in MongoDB:

```json
{
  "ref_time": "2025-01-04T12:00:00",
  "São Paulo": 8.9,
  "Rio de Janeiro": 1.5,
  "Brasília": 1.2
}
```

---
# 9. Coming Soon
-> In the future, container orchestration will be implemented using Docker Swarm, along with a frontend developed in either Streamlit or JavaScript. This interface will access and consume the ETL pipeline through FastAPI or related frameworks.


## 10. Contact

For questions or suggestions, feel free to contact the maintainer.

---

