# Handoff Pos Data Lambda Function

## Table of Contents
1. [About](#about-the-repo)
2. [Setup](#setup)
3. [Code](#code)
4. [Deploy](#deploy)

---
## About
Repo contains:
1. [Python script for AWS Lambda](./src/data_processing_lambda.py)  
    * Python 3.8 code to run pos processing.
2. [Lambda layer deployment script](./src/get_layer_packages.sh)  
    * Bash script for generating virtual environment in Amazon Linux 2
3. Lambda deployment Script  
    * Bash script to upload data_processing_lambda.py to AWS lambda.

The purpose of this AWS lambda function is to process pos data exports into standardized format for insertion into RDS.

---
## Setup

---
## Code

---
## Deploy
1. Generate requirements.txt with all necessary dependencies required to run lambda function.
    * Generate [requirements.txt](./src/requirements.txt) - I recommend using [pipreqs](https://github.com/bndr/pipreqs) to generate requirements.txt. Pipreqs will help avoid including any unnecessary packages etc.

2. Generate site-packages from requirements.txt (these will be saved in directory named python) using Docker Amazon Linux 2.0 container. This will ensure site-package compatiability with AWS Lambda.
    * **ATTENTION: This step may not be necessary if you are developing in a linux environment.** Otherwise, create a virtual enviroment in your linux enviroment rename the site-packages directory as python and move on to step 3.
    * To run [get_layer_packages.sh](./src/get_layer_packages.sh) you will need to install [Docker](https://docs.docker.com/get-docker/) if you don't already have it.
    * Here are more [detailed instructions](https://medium.com/@qtangs/creating-new-aws-lambda-layer-for-python-pandas-library-348b126e9f3e) for generating site-packages using an Amazon Linux 2.0 Docker container. Focus on steps 1 - 4 on the article.  

3. Zip site-packages folder named python.  
```
zip -r pos-processing-layer.zip python 
```

4. Upload AWS lambda layer and attach layer to AWS lambda function.
    * [Deploy lambda layer in AWS console](https://medium.com/faun/how-to-use-aws-lambda-layers-f4fe6624aff1) - Attach lambda layer to AWS lambda function to utlize site-packages/dependencies created in step 2.

5. Zip lambda function.
```
zip -r pos-process-lambda.zip data_processing_lambda.py
```  
*Name zip file whatever you want, but you'll need the name in the next step.*

6. Deploy/upload lambda function.
```
aws lambda update-function-code --function-name pos_data --zip-file fileb://pos-process-lambda.zip
```  
*Function name is the lambda function name and zip-file is the lambda function we just zipped above.*

7. Congrats! Your lambda function has been deployed.

---