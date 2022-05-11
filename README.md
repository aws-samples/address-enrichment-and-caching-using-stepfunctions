# Address Enrichment and Caching Using AWS Step Functions by Leveraging Amazon Location Service

Traditional methods of performing address enrichment on geospatial datasets can be expensive and time consuming. 

Using [Amazon Location Service](https://aws.amazon.com/location/) with [AWS Step Functions](https://aws.amazon.com/step-functions/) for orchestration and with [Amazon DynamoDB](https://aws.amazon.com/dynamodb) for caching in a serverless data processing pipeline, you may achieve significant performance improvements and cost savings on address enrichment jobs that use geospatial data. 

This sample is an evolution to the already available sample, which only uses Lambda functions (can be found [here](https://github.com/aws-samples/amazon-location-service-serverless-address-validation)).

Some of the improvements in this project includes:

- Using [AWS Step Functions](https://aws.amazon.com/step-functions/) for Orchestration
- Using [Amazon DynamoDB](https://aws.amazon.com/dynamodb) as a Naive cache to store location results, which helps improve performance and optimize costs

The repository contains a SAM tempalte for deploying a Serverless Address Enrichment pipeline using:
- [Amazon S3](https://aws.amazon.com/s3/) (for object storage), 
- [AWS Lambda](https://aws.amazon.com/lambda/) (for serverless compute), 
- [AWS Step Functions](https://aws.amazon.com/step-functions/) (for Orchestration), 
- [Amazon DynamoDB](https://aws.amazon.com/dynamodb) (for Caching) 
- [Amazon Location Service](https://aws.amazon.com/location/) (for Geocoding/Reverse Geocoding)

It also uses sample data sourced from publicly available datasets that you can deploy and use to test the application. 

This project addresses the concerns from the customers, how they can improve the performance of their application and at the same time optimize their costs.



## Highlevel Architecture

![image](https://user-images.githubusercontent.com/20495779/167682555-c7656967-f328-4ae9-970a-28999c0f0771.png)


  1.	The *Scatter* Lambda function takes a data set from the S3 bucket labeled *input* and breaks it into equal sized shards. 
  2.	The *Process* Lambda function takes each shard from the *pre-processed* bucket and performs Address Enrichment in parallel calling the [Amazon Location Service Places API](https://docs.aws.amazon.com/location-places/latest/APIReference/Welcome.html) and storing 
  3.	The *Gather* Lambda function takes each shard from the *post-processed* bucket and appends them into a complete dataset with additional address information.


## Deploying the Project
### Prerequistes:

To use the SAM CLI, you need the following tools:
  - [AWS account](https://aws.amazon.com/free/?trk=ps_a134p000003yBfsAAE&trkCampaign=acq_paid_search_brand&sc_channel=ps&sc_campaign=acquisition_US&sc_publisher=google&sc_category=core&sc_country=US&sc_geo=NAMER&sc_outcome=acq&sc_detail=%2Baws%20%2Baccount&sc_content=Account_bmm&sc_segment=438195700994&sc_medium=ACQ-P%7CPS-GO%7CBrand%7CDesktop%7CSU%7CAWS%7CCore%7CUS%7CEN%7CText&s_kwcid=AL!4422!3!438195700994!b!!g!!%2Baws%20%2Baccount&ef_id=Cj0KCQjwsuP5BRCoARIsAPtX_wEmxImXtbdvL3n4ntAafj32KMc_sXL9Z-o8FyXVQzPk7w__h2FMje0aAhOFEALw_wcB:G:s&s_kwcid=AL!4422!3!438195700994!b!!g!!%2Baws%20%2Baccount&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all) 
  - AWS SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
  - Python 3.9 or later - [download the latest of version of python](https://www.python.org/downloads/) 
  - An [AWS Identity and Access Managment](https://aws.amazon.com/iam/) role with appropriate access

### This Sample Includes: 
  - *template.yaml*: Contains the AWS SAM template that defines you applications AWS resources, which includes a Place Index for Amazon Location Service
  - *statemachine/location_service_scatter_gather.asl.yaml*: Contains the Step Functions ASL definition
  - *functions/scatter/*: Contains the Lambda handler logic behind the scatter function and its requirements 
  - *functions/process/*: Contains the Lambda handler logic for the processor function which calls the [Amazon Location Service Places API](https://docs.aws.amazon.com/location-places/latest/APIReference/Welcome.html) to perform address enrichment
  - *functions/gather/*: Contains the Lambda handler logic for the gather function which appends all of processed data into a complete dataset
  - *tests/*: TBD - Needs to contain test cases (Unit and Integration Tests)

### Deploy the Sam-App:
1. Use `git clone https://github.com/aws-samples/address-enrichment-and-caching-using-stepfunctions` to clone the repository to your environment where AWS SAM and python are installed.
2. Use ``https://github.com/aws-samples/address-enrichment-and-caching-using-stepfunctions``to change into the project directory containing the template.yaml file SAM uses to build your application. 
3. If you have Docker installed, you can use ``sam build --use-container``, otherwise, you can use ``sam build`` to build your application using SAM. You should see:

```
Build Succeeded

Built Artifacts  : .aws-sam/build
Built Template   : .aws-sam/build/template.yaml

Commands you can use next
=========================
[*] Invoke Function: sam local invoke
[*] Test Function in the Cloud: sam sync --stack-name {stack-name} --watch
[*] Deploy: sam deploy --guided
```


4. Use `sam deploy --guided` to deploy the application to your AWS account. Enter responses based on your environment:

```
Configuring SAM deploy
======================

        Looking for config file [samconfig.toml] :  Not found

        Setting default arguments for 'sam deploy'
        =========================================
        Stack Name [sam-app]: address-enrichment
        AWS Region [us-west-2]: us-east-1
        #Shows you resources changes to be deployed and require a 'Y' to initiate deploy
        Confirm changes before deploy [y/N]: Y
        #SAM needs permission to be able to create roles to connect to the resources in your template
        Allow SAM CLI IAM role creation [Y/n]: Y
        #Preserves the state of previously provisioned resources when an operation fails
        Disable rollback [y/N]: N
        Save arguments to configuration file [Y/n]: Y
        SAM configuration file [samconfig.toml]: 
        SAM configuration environment [default]: 
```

## Testing the Application

Download the below samples locally, unzip the files, and upload the CSV to your *input S3 bucket* to trigger the adddress enrichment pipeline.

Geocoding: *City of Hartford, CT Business Listing Dataset*
 - https://catalog.data.gov/dataset/city-of-hartford-business-listing
 
Reverse Geocoding: *Miami Housing Dataset*
 - https://www.kaggle.com/deepcontractor/miami-housing-dataset

## Cleanup

In order to avoid incurring any charges, this section talks about cleaning up the AWS resources, which got created when following through this sample. 

### Pre-req:
Make sure you `empty` the following S3 buckets before deleting the Cloud Formation Stack (*as the deletion will fail for non-empty buckets*):
- *input*-`stack-name`-`aws-region`-`aws-accountnumber`
- *raw*-`stack-name`-`aws-region`-`aws-accountnumber`
- *processed*-`stack-name`-`aws-region`-`aws-accountnumber`
- *destination*-`stack-name`-`aws-region`-`aws-accountnumber`


### Method 1:
To delete the resources you created as part of this sample, you can run ``sam delete``:

```
sam delete                                                                                                                                                     
        Are you sure you want to delete the stack address-enrichment in the region us-east-1 ? [y/N]: y
        Are you sure you want to delete the folder address-enrichment in S3 which contains the artifacts? [y/N]: y
        - Deleting S3 object with key address-enrichment/c2710045fb8c4c4d77e47fba2f9754e4
        - Deleting S3 object with key address-enrichment/c5ca75d7c52419e4077a3c030d76d812
        - Deleting S3 object with key address-enrichment/04c2cdceeee06f8998eccf77fc6ffb9b
        - Deleting S3 object with key address-enrichment/f1e2091b2a434fd87f023b603e23fe10
        - Deleting S3 object with key address-enrichment/5a46e427cf72552a09e714f3a5c16461.template
        - Deleting Cloudformation stack address-enrichment

Deleted successfully
```
### Method 2:
Alternatively, you can delete the AWS CloudFormation Stack by going to you AWS Console.


or you can go to the AWS CloudFormation console to delete the stack:




## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

