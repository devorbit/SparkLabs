# SparkLabs

SparkLabs is a playground project designed to help developers quickly get started with Apache Spark. It now includes parallel Scala and Python package structures, with room to grow across both batch and streaming workloads.

## Supported Languages

| Language | Status |
| --- | --- |
| Scala | `✓` |
| Python | `✓` |
| R | `✓` |

## Supported Spark APIs

| API | Scala | Python | R |
| --- | --- | --- | --- |
| RDD | `✓` | `✓` | `-` |
| DataFrame | `✓` | `✓` | `✓` |
| Dataset | `✓` | `✓` | `-` |
| SQL | `✓` | `✓` | `✓` |

## Features

- **Structured Packages**: Source code is organized under `batch` and `streaming` for future expansion.
- **Scala and Python**: The sales examples now exist in both languages with matching package layouts.
- **RDD Support**: Sales examples now include `rdd` implementations alongside higher-level Spark APIs.
- **SQL and DataFrame Styles**: Sales examples are split into `dataframe` and `sql` implementations for the same business questions.
- **Dataset Style**: Sales examples also include `dataset` implementations, using typed Datasets in Scala and dataset-style record mapping in Python.
- **Sales Sample Data**: Includes simple sales-related datasets to build a mental model of data processing.
- **Sample DataFrame Examples**: Provides Spark session initialization and sample transformations for hands-on learning.
- **Additional Libraries**: Integrates DataFlint for enhanced Spark UI visualization and Deequ for Scala data quality testing.

## Project Structure

```text
SparkLabs/
├── data/
│   └── sales/
├── python/
│   ├── requirements.txt
│   └── sparklabs/
│       ├── batch/
│       │   └── sales/
│       │       ├── dataframe/
│       │       ├── dataset/
│       │       ├── rdd/
│       │       └── sql/
│       └── streaming/
├── r/
│   └── sparklabs/
│       └── batch/
│           └── sales/
│               ├── dataframe/
│               ├── dataset/
│               ├── rdd/
│               └── sql/
├── scala/
│   ├── build.sbt
│   └── src/
│       └── main/
│           └── scala/
│               └── io/
│                   └── devorbit/
│                       └── sparklabs/
│                           ├── batch/
│                           │   └── sales/
│                           │       ├── dataframe/
│                           │       ├── dataset/
│                           │       ├── rdd/
│                           │       └── sql/
│                           └── streaming/
└── README.md
```

- `data/sales/`: Shared CSV datasets used by both language implementations.
- `scala/src/main/scala/io/devorbit/sparklabs/batch/sales/dataframe/`: Scala DataFrame-based sales examples.
- `scala/src/main/scala/io/devorbit/sparklabs/batch/sales/dataset/`: Scala Dataset-based sales examples.
- `scala/src/main/scala/io/devorbit/sparklabs/batch/sales/rdd/`: Scala RDD-based sales examples.
- `scala/src/main/scala/io/devorbit/sparklabs/batch/sales/sql/`: Scala SQL-based sales examples.
- `scala/src/main/scala/io/devorbit/sparklabs/streaming/`: Scala streaming area for future examples.
- `r/sparklabs/batch/sales/dataframe/`: R SparkR DataFrame-based sales examples.
- `r/sparklabs/batch/sales/sql/`: R SparkR SQL-based sales examples.
- `r/sparklabs/batch/sales/dataset/`: R notes for unsupported native Dataset API.
- `r/sparklabs/batch/sales/rdd/`: R notes for unsupported native RDD API.
- `python/sparklabs/batch/sales/dataframe/`: Python DataFrame-based sales examples.
- `python/sparklabs/batch/sales/dataset/`: Python dataset-style sales examples.
- `python/sparklabs/batch/sales/rdd/`: Python RDD-based sales examples.
- `python/sparklabs/batch/sales/sql/`: Python SQL-based sales examples.
- `python/sparklabs/streaming/`: Python streaming area for future examples.

## Setup Instructions

### Prerequisites

- Java JDK (version 8 or higher)
- Scala SDK for the Scala project
- Python 3.10+ for the Python project
- R with SparkR for the R project
- IntelliJ IDEA with Scala plugin installed, or your preferred editor

### Steps

1. Clone or download the SparkLabs project to your local machine.

2. Open IntelliJ IDEA.

3. Go to **File > Open** and select the SparkLabs project directory.

4. IntelliJ IDEA should automatically detect it as an SBT project if the Scala plugin is installed. If not, ensure the Scala plugin is enabled.

5. Go to **File > Project Structure**.

6. Under **Project Settings > Project**, select a Java SDK and Scala SDK under the **Project SDK** and **Project language level**.

7. Under **Project Settings > Modules**, select the Scala module and mark `scala/src/main/scala` as the source directory by right-clicking it and selecting **Mark Directory as > Sources Root**.

8. If prompted, refresh the SBT project. You can do this by clicking the refresh icon in the SBT tool window or via the tooltip.

9. Once set up, you can start running Scala applications from `io.devorbit.sparklabs.batch.sales.rdd`, `io.devorbit.sparklabs.batch.sales.dataframe`, `io.devorbit.sparklabs.batch.sales.dataset`, or `io.devorbit.sparklabs.batch.sales.sql`.

### Python setup

1. Create and activate a virtual environment inside [python](/Users/jayantkumar/Workspace/Code/SparkLabs/python).
2. Install dependencies with `pip install -r requirements.txt`.
3. Set `PYTHONPATH=.` from the `python/` directory.
4. Run a DataFrame sample with `python -m sparklabs.batch.sales.dataframe.top_selling_products`.
5. Run an RDD sample with `python -m sparklabs.batch.sales.rdd.top_selling_products`.
6. Run a dataset-style sample with `python -m sparklabs.batch.sales.dataset.top_selling_products`.
7. Run a SQL sample with `python -m sparklabs.batch.sales.sql.top_selling_products`.

### R setup

1. Install Apache Spark locally. Do not try to install `SparkR` from CRAN; it ships with Spark.
2. Set `SPARK_HOME` to your Spark installation root so the R scripts can load `SparkR` automatically.
3. In IntelliJ, add `SPARK_HOME` to the R run configuration environment if you want to run the scripts from the IDE.
4. From the repo root, run a DataFrame sample with `Rscript r/sparklabs/batch/sales/dataframe/top_selling_products.R`.
5. Run a SQL sample with `Rscript r/sparklabs/batch/sales/sql/top_selling_products.R`.

## Contribution Guide

We welcome contributions to SparkLabs! To contribute:

1. Fork the repository on GitHub.

2. Create a new branch for your feature or bug fix: `git checkout -b feature/your-feature-name`.

3. Make your changes and commit them: `git commit -m "Add your commit message"`.

4. Push to your branch: `git push origin feature/your-feature-name`.

5. Open a Pull Request on GitHub, describing your changes and why they should be merged.

Please ensure your code follows the project's coding standards and includes appropriate tests.
