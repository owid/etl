A step used to mark dependencies on HTTPS resources. The path is interpreted as an HTTPS url, and a HEAD request is made against the URL and checked for its `ETag`. This can be used to trigger a rebuild each time the resource changes.


!!! warning "This step is rarely used"

    Currently it is only used to import some COVID-19 data, which is published by us in another project.

    ```yaml title="dag/health.yml"
    data://garden/owid/latest/covid:
    - etag://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv
    ```