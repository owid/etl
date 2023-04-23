In some cases, a source or publisher does not allow for re-publication of their data. When that happends, but we still want to use their data in ETL, we use _private steps_.

Private steps work the same way as regular steps and are the only difference is that the data from these steps is not made available in our catalog and APIs.

In the [DAG](../../design/dag.md), these steps appear with the prefix `data-private://`.


!!! example
    The garden step for the "Drug Use Disorders - Global Burden of Disease Study 2019 (GBD 2019)" dataset by the IHME is private. Their URI looks like:
    ```
    data-private://garden/ihme_gbd/2023-03-29/gbd_drug_disorders
    ```

!!! info "[How to implement a private step](../../../tutorials/private-import.md)"