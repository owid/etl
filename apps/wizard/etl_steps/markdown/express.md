This step is for _express_ ETL step generation. It should only be used when your data pipeline is simple and linear: meadow → garden → grapher, with one dataset.

This step will:

1. **Create all meadow, garden and grapher** step files.

2. **Once generated, you should edit them and add the necessary code** to load, process and save the data.

3. **Once happy, run the steps with `etl run <step-name>`**.
