#Pull the base image for Python 3.11.16 slim version
FROM python:3.11.16-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app but for the image
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Make port 5000 available to the machine outside this container
EXPOSE 5000

# Run app.py when the container launches
CMD ["python", "app.py"]
