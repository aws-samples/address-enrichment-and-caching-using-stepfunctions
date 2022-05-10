#  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

import json
import boto3
import pandas as pd
import io
import numpy as np
import urllib.parse
import os
import time

###  This function gets .csv file from an "input" bucket,
###  then splits the file into shards of equal size,
###  renames the files, and puts them into a "raw" bucket

# input_bucket = os.environ.get('INPUT_BUCKET')
destination_bucket = os.environ.get('RAW_SHARDS_BUCKET')
# destination_bucket = '2-scatter-gather-1-us-east-2-953053785568'
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')


def lambda_handler(event, context):
    """
    Lambda function to generate Map

    Parameters
    ----------
    event: dict, required
        S3 Object events as input

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: response from Step functions start execution
    """
    #get object from s3 and read the data
    print(event)
    print(type(event))
    source_bucket = event['Payload']['detail']['bucket']['name']
    source_object = event['Payload']['detail']['object']['key']

    if source_object.endswith(".csv"):
        # source_object = urllib.parse.unquote_plus(event['detail']['object']['key'], encoding='utf-8')
        
        response = s3_client.get_object(Bucket=source_bucket, Key=source_object)
        data = pd.read_csv(response.get("Body"))
        df = data.dropna(thresh=2)

        # break up the dataset into x number of shards (to avoid the default 50 Request Per Second API Throtling restrictions for AWS Location Service, this is left at 4)
        data_set_size = round(len(df))
        number_of_shards = 10
        shard_length = int(data_set_size / number_of_shards)
        shards = np.array_split(df, number_of_shards)

        # using a for loop, take each data shard and write it individually to s3 with a unique suffix identifier of "_SHARD_X"
        count = 0
        response_lambda = {}
        response_lambda['Payload']={}
        response_lambda['Payload']['Shards']=[]
        # response_lambda['Payload']['Bucket']=destination_bucket
        print(response)
        for i in shards:
            if count < (number_of_shards-1):
                with io.StringIO() as csv_buffer:
                    shards[count].to_csv(csv_buffer, index=False)
                    count += 1  
                    number_label = str(count)
                    shard = source_object[:-4] + "_SHARD_" + number_label + ".csv"
                    response = s3_client.put_object(
                        Bucket=destination_bucket, Key=shard,
                        Body=csv_buffer.getvalue()
                    )
                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if status == 200:
                        print(f"Successful S3 put_object response. Status - {status}")
                    else:
                        print(f"Unsuccessful S3 put_object response. Status - {status}")
                    
                    print(shard)
                    print(response_lambda['Payload']['Shards'])
                    # print(response['Shards']['number_label'] = )
                    # response['Shards']['number_label'] = shard
                    item = {'bucket': destination_bucket, 'shard': shard}
                    response_lambda['Payload']['Shards'].append(item)
                print(number_of_shards)
                print(count)
            # wait 5 seconds, then take the last shard and write it to S3 with a unique suffix identifier of "_SHARD_LAST"
            else:
                time.sleep(5)
                with io.StringIO() as csv_buffer:
                    shards[-1].to_csv(csv_buffer, index=False)
                    shard = source_object[:-4] + "_SHARD_" + "LAST" + ".csv"
                    response = s3_client.put_object(
                        Bucket=destination_bucket, Key=source_object[:-4] + "_SHARD_" + "LAST" + ".csv",
                        Body=csv_buffer.getvalue()
                    )
                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if status == 200:
                        print(f"Successful S3 put_object response. Status - {status}")
                    else:
                        print(f"Unsuccessful S3 put_object response. Status - {status}")
                    # response['Shards']['number_label'] = shard
                    item = {'bucket': destination_bucket, 'shard': shard}
                    response_lambda['Payload']['Shards'].append(item)
        
        return(response_lambda)
    else:
        raise Exception("Error: Step Functions can only process a CSV file")