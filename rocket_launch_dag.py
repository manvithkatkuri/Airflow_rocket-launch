import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define the DAG
dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),  
    schedule_interval=None,  
)


#get syntaxes from airflow documentation
# Task 1: Download launches.json using BashOperator
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",  # noqa: E501
    dag=dag,
)



# Task 2: PythonOperator to download images
def _get_pictures():
    # Ensure directory /opt/airflow/images exists
    image_dir = pathlib.Path("/opt/airflow/images")
    image_dir.mkdir(parents=True, exist_ok=True)

    # Load the launches.json file
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        # Extract image URLs from the JSON file
        image_urls = [launch["image"] for launch in launches["results"] if "image" in launch]
        for image_url in image_urls:
            try:
                # Download the image
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = image_dir / image_filename
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


#Task2: Images must be downloaded and stored in images folder
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

# Task 3: Notify the number of images downloaded using BashOperator
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /opt/airflow/images/ | wc -l) images."',
    dag=dag,
)

# Set task dependencies
download_launches >> get_pictures >> notify
