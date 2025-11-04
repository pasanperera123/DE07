# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Copy code into Lambda's default directory
COPY . ${LAMBDA_TASK_ROOT}

# Install system dependencies required to build numpy/pandas
RUN pip install --upgrade pip \
 && pip install --only-binary=:all: -r requirements.txt

# Set the Lambda handler (module.function)
CMD ["main.lambda_handler"]

# docker build -t my-lambda-scraper .