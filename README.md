# Airflow, DBT, Snowflake, and Docker Pipeline
![image](https://github.com/user-attachments/assets/a48dc27d-1f7f-406c-a3ed-04cc9c66d39f)

## Overview

This repository demonstrates a modern data pipeline integrating Apache Airflow, DBT (Data Build Tool), Snowflake, and Docker. The **pipeline** is designed to perform efficient ETL (Extract, Transform, Load) processes while emphasizing scalability, maintainability, and performance. It serves as a framework for automating data workflows, transforming raw data into meaningful insights, and managing data assets effectively.

---

## Features

- **Orchestration with Airflow**: Schedule and monitor workflows seamlessly.
- **Data Transformation with DBT**: Perform reliable, modular transformations and testing of your data models.
- **Snowflake Integration**: Scalable, cloud-based storage and computation for analytical workloads.
- **Containerization with Docker**: Ensures consistency across environments and simplifies deployment.
- **Extensible Architecture**: The architecture can be tailored to support additional use cases or integrations.

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

---

### Support
If you encounter issues or have questions about this project, feel free to [open an issue](https://github.com/ahmedbessar/pipeline_elasticsearch_snowflake/issues) on GitHub.

### Contact
Ahmed Bessar 
Email: ahmedbessar28@gmail.com  
GitHub: [@ahmedbessar](https://github.com/ahmedbessar)
