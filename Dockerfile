# Use Python 3.13.0a4-slim as the base image
#FROM python:3.13.0a4-slim

FROM python:3.9

RUN pip install --no-cache-dir pyarrow

# Install build dependencies
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     python3-dev \
#     && rm -rf /var/lib/apt/lists/*
    
# Set the working directory in the container
WORKDIR /usr/project/d2p_production

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install dependencies
#RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir -r requirements.txt 

# Copy the content of the local src directory to the working directory
COPY . .

# Command to run the Python script
CMD ["python", "src/test_utils_functions.py"]
