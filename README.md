# Airflow, DBT, Snowflake, and Docker Pipeline

## Overview

This repository contains a robust data pipeline that leverages **Apache Airflow**, **DBT (Data Build Tool)**, **Snowflake**, and **Docker** to orchestrate and transform data. It demonstrates the integration of modern tools and frameworks for ETL (Extract, Transform, Load) processes with an emphasis on scalability, maintainability, and performance.

---

## Features

- **Orchestration with Airflow**: Automates and schedules data workflows.
- **Data Transformation with DBT**: Builds, tests, and documents data models.
- **Snowflake Integration**: Efficient data storage and query processing.
- **Containerization with Docker**: Ensures consistent and portable environments.
- **Extensible Architecture**: Easily adaptable for different data engineering use cases.

---

## Project Structure

Airflow_DBT_Snowflake_Docker/ │ ├── docker-compose.yml # Docker configuration file ├── dags/ # Airflow DAGs folder │ ├── create_layers.py # DAG for data layer creation │ └── pycache/ # Compiled Python files (ignored) │ ├── dbtlearn/ # DBT project folder │ ├── dbt_project.yml # DBT project configuration │ ├── models/ # DBT models folder │ │ └── mart_classified_reports/ │ │ └── Brands_traffic.sql # Sample DBT model │ ├── seeds/ # DBT seed files │ │ └── full_moon_dates.csv # Example seed data │ └── target/ # Compiled and manifest files (ignored) │ ├── docker/ # Docker-specific resources │ ├── dbt/ # DBT Docker setup │ │ ├── Dockerfile # DBT Dockerfile │ │ ├── my_password.txt # Sensitive info (ignored) │ │ ├── requirements.txt # Python dependencies │ └── dags/ # DAGs for containerization │ ├── scripts/ # Helper scripts for pipeline tasks │ ├── extraction_load.py # Data extraction and loading script │ ├── incremental_load.py # Incremental loading script │ ├── .env # Environment variables (ignored) │ └── snowflake_config/ # Snowflake SQL configuration files └── first_step_sf_config.sql # Initial Snowflake setup
