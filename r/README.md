# SparkLabs R

This module uses SparkR and mirrors the sales examples with working `dataframe`
and `sql` scripts.

`dataset` and `rdd` directories are included for layout parity, but SparkR does
not expose native Dataset or RDD APIs.

Run examples from the repo root, for example:

```sh
Rscript r/sparklabs/batch/sales/dataframe/top_selling_products.R
Rscript r/sparklabs/batch/sales/sql/top_selling_products.R
```
