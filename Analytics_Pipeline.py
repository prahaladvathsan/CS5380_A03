# Importing necessary libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os
import random
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
import apache_beam as beam
from airflow.models import Variable
import numpy as np
import pandas as pd
import urllib
import shutil
from datetime import datetime, timedelta
import geopandas as gpd
from geodatasets import get_path
import logging
from ast import literal_eval as make_tuple
import matplotlib.pyplot as plt

### TASK 1: Data Fetch Pipeline

# Function to get data
def collect_data(year: int, destination_dir: str) -> None:
    """
    Fetches data files for the specified year and stores them in the destination directory.

    Parameters:
    - year (int): Year for which to fetch data.
    - destination_dir (str): Directory where the data files will be stored.

    Returns:
    None
    """
    base_url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'
    try:
        response = requests.get(base_url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        # Further implementation...
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")

    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find("table").find_all("tr")[2:-2]

    # Initialize an empty list to store the file names
    fileName = []

    # Define the total number of files to fetch
    total = 10

    # Fetch the file names
    for i in range(total):
        index = random.randint(0, len(rows))
        data = rows[index].find_all("td")
        fileName.append(data[0].text)
    
    # Write the binary data onto the local directory
    for name in fileName:
        newUrl = url+name
        response = requests.get(newUrl)
        open(name,'wb').write(response.content)

    # Zip the files
    with ZipFile('/root/airflow/DAGS/weather.zip','w') as zip:
        for file in fileName:
            zip.write(file)

# Define the default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "local_climatological_data_pipeline",
    default_args=default_args,
    description="A pipeline to fetch, process, and visualize climatological data",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the collect_data task
collect_data_task = PythonOperator(
    task_id="collect_data",
    python_callable=collect_data,
    op_kwargs={"year": 2023, "destination_dir": "/path/to/destination"},
    dag=dag,
)