# Use a slim Python base image for a smaller footprint
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
# This is a key step for Docker's build cache.
# It ensures the 'pip install' layer is only rebuilt if requirements.txt changes.
COPY requirements.txt .

# Install all the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# The rest of the code will be mounted as a volume in docker-compose.yml
# No need for a COPY . . instruction here.