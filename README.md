# SparkLabs

SparkLabs is a playground project designed to help developers quickly get started with Apache Spark. It currently supports Scala and is planned to extend support for Python and R in the future. This project emphasizes ease of setup, providing sample sales data, and including examples to facilitate learning and experimentation with Spark.

## Features

- **Easy Setup**: Minimal dependencies to allow developers to start learning Spark quickly.
- **Sales Sample Data**: Includes simple sales-related datasets to build a mental model of data processing.
- **Sample Dataframe Examples**: Provides Spark session initialization and sample DataFrame operations for hands-on learning.
- **Additional Libraries**: Integrates DataFlint for enhanced Spark UI visualization and Deequ for data quality testing.

## Project Structure

```
SparkLabs/
├── data/
│   └── sales/
│       ├── customers.csv
│       ├── deliveries.csv
│       ├── events_stream.csv
│       ├── inventory.csv
│       ├── order_items.csv
│       ├── orders.csv
│       ├── products.csv
│       ├── promotions.csv
│       ├── returns.csv
│       ├── sales_reps.csv
│       └── stores.csv
├── src/
│   └── main/
│       └── scala/
│           └── io/
├── build.sbt
└── project/
    └── build.properties
```

- `data/`: Contains sample sales data in CSV format.
- `src/`: Source code directory for Scala applications.
- `build.sbt`: SBT build file for managing dependencies and project configuration.

## Setup Instructions

### Prerequisites

- Java JDK (version 8 or higher)
- Scala SDK
- IntelliJ IDEA with Scala plugin installed

### Steps

1. Clone or download the SparkLabs project to your local machine.

2. Open IntelliJ IDEA.

3. Go to **File > Open** and select the SparkLabs project directory.

4. IntelliJ IDEA should automatically detect it as an SBT project if the Scala plugin is installed. If not, ensure the Scala plugin is enabled.

5. Go to **File > Project Structure**.

6. Under **Project Settings > Project**, select a Java SDK and Scala SDK under the **Project SDK** and **Project language level**.

7. Under **Project Settings > Modules**, select the module (e.g., `scala`), and mark the `src/main/scala` directory as the source directory by right-clicking it and selecting **Mark Directory as > Sources Root**.

8. If prompted, refresh the SBT project. You can do this by clicking the refresh icon in the SBT tool window or via the tooltip.

9. Once set up, you can start running your Spark applications. Ensure you have Spark dependencies configured in `build.sbt`.

## Contribution Guide

We welcome contributions to SparkLabs! To contribute:

1. Fork the repository on GitHub.

2. Create a new branch for your feature or bug fix: `git checkout -b feature/your-feature-name`.

3. Make your changes and commit them: `git commit -m "Add your commit message"`.

4. Push to your branch: `git push origin feature/your-feature-name`.

5. Open a Pull Request on GitHub, describing your changes and why they should be merged.

Please ensure your code follows the project's coding standards and includes appropriate tests.
