# Server-side API


As a first step you have to crawl the catalog to get data and metadata into `api/duck.db` file. Check out these scripts to
include / exclude more datasets from the garden channel

```
python api/reindex_data.py
python api/reindex_metadata.py
```

Once you have it there, run the API

```
uvicorn main:app --reload
```

with the API running, you can start the web UI

```
python api/demo.py
```

Sample requests:

```
http POST http://localhost:8000/sql sql=="PRAGMA show_tables;" type==csv
http POST http://localhost:8000/sql sql=="select * from garden_ggdc_2020_10_01_ggdc_maddison_maddison_gdp limit 10;" type==csv
```
