# Use the official Airflow image
FROM apache/airflow:latest

# Set the working directory to the Airflow home directory
WORKDIR /usr/local/airflow

# Copy the DAGs from the local machine to the Airflow DAGs directory
COPY ./dags /usr/local/airflow/dags
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt
# Set the command to run Airflow
CMD ["bash", "-c", "airflow db migrate && airflow webserver"]


