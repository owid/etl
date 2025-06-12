To upgrade the version of Python used in the ETL pipeline, follow these steps on the ETL terminal:

1. Remove the current environment configuration:
   ```
   rm -rf .venv
   ```
2. Rebuild the environment with the new Python version (replace xx.x with the desired version):
   ```
   PYTHON_VERSION=3.xx.x make .venv
   ```

The ETL currently supports versions 3.9 to 3.12.
