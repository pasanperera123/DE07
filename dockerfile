# Mentioning amd64 specifically cuz on the mac it defaulted to arm64 and failed in the Azure container.
# It seems the SQL driver-related stuff won't install on ARM.
FROM --platform=linux/amd64 python:3.11-slim-bookworm



# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Run main.py when the container launches
CMD ["python3", "main.py"]

# docker build -t scraper .
# docker run -p 80:80 scraper