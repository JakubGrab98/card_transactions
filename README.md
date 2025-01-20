
# Card Transactions Project

## Overview

This project is designed to process and analyze credit card transaction data. It includes tools for data extraction, transformation, loading (ETL), and visualization. The system integrates with Apache Spark, Apache Airflow, MinIO Object Store and Streamlit to provide a seamless workflow for data engineers and analysts.

## Project Structure

```
card_transactions/
├── conf/                   # Configuration files
├── .env                    # Application environemt variables
├── dags/                   # Apache Airflow DAGs
├── etl/                    # ETL scripts for data processing
├── requirements/           # Python dependencies
├── streamlit_app/          # Streamlit application for data visualization
├── .gitignore              # Git ignore file
├── Dockerfile              # Docker image definition
├── airflow.env             # Airflow environment variables
├── docker-compose.yaml     # Docker Compose configuration
├── load_data.py            # Script to load raw data
├── spark-minio-entrypoint.sh # Entrypoint for Spark with MinIO
```

## Prerequisites

Ensure you have the following installed on your system:

- **Python 3.8 or higher**
- **Docker**: For containerization

## Setup Instructions

### 1. Clone the Repository

Clone the repository to your local system:

```bash
git clone https://github.com/JakubGrab98/card_transactions.git
cd card_transactions
```

### 2. Build and Start Docker Containers

Run the following command to set up the environment using Docker Compose:

```bash
docker-compose up --build
```

This command initializes services like Apache Airflow and Apache Spark.

### 3. Install Dependencies (Optional)

If running the project locally without Docker, install dependencies:

```bash
pip install -r requirements/requirements.txt
```

## Usage

### 1. Load Transaction Data

Use the `load_data.py` script to load your credit card transaction data:

```bash
python load_data.py --file_path path/to/your/data.csv
```

Ensure the data file is in CSV format and meets the expected schema.

### 2. ETL Workflow

The ETL process is managed using Apache Airflow. Access the Airflow web interface at `http://localhost:8080` to trigger and monitor DAGs for:

- Extracting raw data
- Transforming it into usable formats
- Loading it into a target database or system

### 3. Data Visualization

The project includes a Streamlit application for interactive data visualization. Launch the app using:

```bash
streamlit run streamlit_app/app.py
```

Navigate to the provided local URL to explore your transaction data.

## License

This project is licensed under the MIT License. For details, see the [LICENSE](LICENSE) file.
