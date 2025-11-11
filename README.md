🧩 ELT Project v1
Overview

ELT Project v1 is a personal data engineering project built to test and showcase my skills in building modern data pipelines.
The project demonstrates how to extract data from external APIs, load it into a cloud data warehouse (Google BigQuery), and transform it across different modeling layers for analytical use.

Project Objective

The goal is to design a simplified end-to-end ELT pipeline that can:

1.Extract data from multiple APIs — primarily Open Brewery DB

2.Load and store raw data in Google BigQuery for persistence.

3.Transform the data through staging → core → analytics layers using dbt.

4.Deliver clean, analysis-ready datasets for data analysts and business teams.


Key Concepts Demonstrated


| Layer          | Tool / Technology                       | Description                               |
| -------------- | --------------------------------------- | ----------------------------------------- |
| Extraction     | Python (`requests`, custom scripts)     | Collects data from public APIs            |
| Storage        | Google BigQuery                         | Cloud data warehouse                      |
| Transformation | dbt                                     | Data modeling, lineage, and documentation |
| Orchestration  | Python / CLI                            | Manual or scheduled runs                  |
| Source APIs    | Open Brewery DB, QuickBooks (test data) | Data sources for ingestion                |

