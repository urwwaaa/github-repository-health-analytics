# GitHub Repository Health Analytics

Data engineering project for analyzing the health and development activity of open-source GitHub repositories using Apache Spark, Medallion Architecture, and Power BI.

## Project Concept
This project will analyze the development activity and maintenance patterns of three major open-source GitHub repositories.

The repositories selected are:
- apache/spark
- pandas-dev/pandas
- scikit-learn/scikit-learn

The analysis will focus on:
- Commits
- Issues
- Pull Requests
- Contributor activity

## Data Source
The project will use the GitHub REST API to acquire public repository data.

## Ingestion Pattern

### Full Load
For the initial load, the project will collect approximately one year of historical GitHub activity for the selected repositories.

Planned historical period:
- October 2025 to September 2026

The full load will include:
- Commits
- Issues
- Pull Requests
- Contributor activity

### Incremental Load
After the initial full load, the pipeline will run periodically and fetch only new or updated records from the GitHub REST API.

A timestamp watermark will be maintained so the pipeline can continue from the last successful ingestion time instead of reloading the entire history.
