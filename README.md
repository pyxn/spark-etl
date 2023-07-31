

# High-Performance ETL Solution with PySpark and Docker

This project introduces a robust and easy-to-scale data processing pipeline that uses PySpark for fast data handling and Docker for flexible, consistent runtimes. The pipeline takes in CSV data, performs necessary transformations, and outputs the results as CSV. With Docker, this pipeline ensures the same results regardless of the runtime environment.

## Prerequisites

Before you start, make sure Docker is installed and set up properly on your system.

## Project Files

- `transform.py`: This script does all the ETL (extract, transform, load) work.
- `Dockerfile`: This file tells Docker how to create the right environment for the script. This environment includes Python 3.9 and Apache Spark 3.1.2.
- `dbuild.sh`: This shell script automates building and running the Docker container.
- `config.ini`: This file lets you customize the input/output directories and lookup table paths.

## How to Set Up the Docker Environment

### :warning: Important Note :warning:
:fire: **The `dbuild.sh` script stops and removes ALL active Docker containers and images on your system.** :fire: Please be careful when using this script. If you have important Docker containers running, you should modify this script to avoid stopping and removing them.

### How to Build and Run the Docker Image

1. In your terminal, go to the project directory.
2. Make the shell script executable with this command:
   ```bash
   chmod +x dbuild.sh
   ```
3. Run the `dbuild.sh` script to build and run the Docker image. Don't forget to replace `my_image_name` with the name you want for your image:
   ```bash
   ./dbuild.sh my_image_name
   ```

## How to Configure the Pipeline

You can modify the `config.ini` file to set your data sources and destinations. In this file, you should set paths for:

- `DataDir`: The directory with your input data.
- `OutputPathmatics` and `OutputVivvix`: Where the pipeline should save the Pathmatics and Vivvix data.
- Lookup table paths: Where to find the lookup tables that the pipeline uses to join data.

## How to Use the Pipeline

Before you run this pipeline, make sure Docker is installed and configured correctly on your system.

:loudspeaker: **This pipeline is designed to run on a Spark cluster.** You can use standalone mode for testing and development. However, for real-world use, you should set up this pipeline on a full Spark cluster.

## Understanding `dbuild.sh` and When to Use It

The `dbuild.sh` script makes sure that each Docker image build and run starts from scratch by stopping and removing all Docker containers and images. This clean start is important for making sure that tests are consistent and reproducible.

However, the script is designed to be very thorough and is best used in controlled environments. If you use Docker for many projects on your local machine or in shared or production environments, this script could delete important data or disrupt services. So, it's important to understand what this script does and adjust it as needed.

## Collaborate and Contribute

Your input is important! Feel free to point out issues, suggest improvements, or provide feedback to enhance the pipeline.

---