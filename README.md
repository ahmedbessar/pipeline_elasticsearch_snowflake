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


---

## Prerequisites

To use or extend this project, you need the following installed on your system:

1. **Docker** (version 20.10 or later)
2. **Docker Compose**
3. **Python** (version 3.7 or later)
4. **DBT** (version 1.0 or later)
5. **Snowflake** account and credentials
6. **Git** (for version control)

---

## Getting Started

### Step 1: Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/ahmedbessar/pipeline_elasticsearch_snowflake.git
cd pipeline_elasticsearch_snowflake
```

### Step 2: Set Up Environment Variables
Create an .env file inside the scripts/ folder.
Add your environment variables (e.g., database credentials, API keys, etc.) to .env:
```
DB_HOST=<your-database-host>
DB_USER=<your-username>
DB_PASSWORD=<your-password>
DB_NAME=<your-database-name>
```

### Step 3: Start Docker Containers
Run the following command to spin up all necessary services (Airflow, DBT, etc.):
```bash
docker-compose up --build
```

### Step 4: Access the Services
- Airflow Webserver: http://localhost:8080
- Snowflake: Connect via your Snowflake account.
- DBT CLI: Interact with DBT inside the Docker container.


### Step 5: Run a Sample Workflow
Access the Airflow UI.
Trigger the create_layers.py DAG to test the pipeline.

### Contact
Ahmed Bessar 
Email: ahmedbessar28@gmail.com  
GitHub: @ahmedbessar  
 
