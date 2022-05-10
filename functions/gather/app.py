#  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

import json
import pandas as pd
import boto3
import io
import os

###  This function is triggered when a file with suffix _LAST.csv
###  is input into the "processed" bucket. This function then takes
###  all of the shards from the "processed" bucket and pieces them t
###  together to create a complete processed data set. Then this function 
###  writes that dataset to a "destination" bucket

s3_client = boto3.client('s3')
destination_bucket = os.environ.get('DESTINATION_BUCKET')
process_shards_bucket = os.environ.get("PROCESSED_SHARDS_BUCKET")

def lambda_handler(event, context):
    bucket_name = process_shards_bucket
    payload = event["Payload"]
    
    list_of_shards = []
    for item in payload:
        print (item["shard"])
        list_of_shards.append(item["shard"])
    
    #Create the data frame (final_doc) using the first shard
    final_doc = pd.DataFrame()
    first_object_response = response_2 = s3_client.get_object(Bucket=bucket_name, Key=list_of_shards[0])
    final_doc = pd.read_csv((first_object_response.get("Body")))
    count_2 = 1
    length=len(list_of_shards)

    #Write a new file (final_doc) by iterating through the list of files in the bucekt
    for i in list_of_shards[1:length]:
        response_2 = s3_client.get_object(Bucket=bucket_name, Key=i)
        data_1 = pd.read_csv((response_2.get("Body")))
        data_1 = data_1.dropna(thresh=2)
        final_doc = final_doc.append(data_1)
        count_2 +=1
    
    # output_file_name = 'output.csv'
    output_file_name = list_of_shards[0][:-12]+"/"+"PROCESSED_DATA_"+list_of_shards[0][:-12]+".csv"
    response_lambda = {}
    #Put new File back to S3
    with io.StringIO() as csv_buffer:
        final_doc.to_csv(csv_buffer, index=False)
        response_3 = s3_client.put_object(
            Bucket=destination_bucket, Key="processed_data"+"/"+output_file_name, Body=csv_buffer.getvalue()
        )
        status = response_3.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
            response_lambda['Payload']={"status": "Processed file uploaded to: "+ destination_bucket + " as "+ output_file_name}
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
            response_lambda['Payload']={"status": status}
    return(response_lambda)
