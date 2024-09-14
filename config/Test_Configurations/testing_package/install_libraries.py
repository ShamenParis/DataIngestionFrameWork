import subprocess
import sys
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('availability_check').getOrCreate()
dbutils = DBUtils(spark)

def install_requirements(requirements_file):
    """
    Install packages from a requirements.txt file.

    Parameters:
    requirements_file (str): Path to the requirements.txt file.
    """
    try:
        # Check if pip is installed
        subprocess.check_call([sys.executable, '-m', 'pip', '--version'])
        # Install the packages
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', requirements_file])
        print(f"All packages from {requirements_file} have been installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while trying to install the packages: {e}")