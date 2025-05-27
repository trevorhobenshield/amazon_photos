# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install system dependencies that might be needed by Python packages
# For example, if pandas or numpy need them for compilation from source.
# Often not needed with wheels from PyPI, but good practice for some cases.
# RUN apt-get update && apt-get install -y --no-install-recommends some-lib && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
# Ensure requirements.txt includes:
# fastapi
# uvicorn[standard]
# pandas
# httpx
# aiofiles
# orjson
# numpy
# psutil
# (These are dependencies of amazon_photos lib + the web framework)
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY main.py .
COPY frontend.html .
COPY amazon_photos/ ./amazon_photos/

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define the command to run your app using uvicorn
# Run Uvicorn with --host 0.0.0.0 to be accessible externally
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
