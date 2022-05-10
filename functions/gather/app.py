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
# destination_bucket = "4-scatter-gather-1-us-east-2-953053785568"
# process_shards_bucket = "3-scatter-gather-1-us-east-2-953053785568"

def lambda_handler(event, context):
    #Get all the objects in the bucket
    # bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    # response = s3_client.list_objects_v2(Bucket=bucket_name)
    print (event)
    print (type(event))
    # bucket_name = event["Payload"][0]["bucket"]
    # shard = event["Payload"][0]["shard"]
    bucket_name = process_shards_bucket
    payload = event["Payload"]
    
    list_of_shards = []
    for item in payload:
        print (item["shard"])
        list_of_shards.append(item["shard"])


    
    # #Identify the name of the most recent object uploaded to the bucket
    # all = response['Contents']
    # latest = max(all, key=lambda x: x['LastModified'])
    # latest_object_name = (latest['Key'])
 
    # #Get all the shards that share the same prefix (file name)
    # response_1 = s3_client.list_objects_v2(Bucket=bucket_name, Prefix= latest_object_name[:-12])['Contents']
    
    # #Create a list of all the shards
    # list_of_shards = []
    # count = 0
    # for i in response_1:
    #     record = response_1[count]['Key']
    #     list_of_shards.append(record)
    #     count += 1
    
    #Create the data frame (final_doc) using the first shard
    final_doc = pd.DataFrame()
    first_object_response = response_2 = s3_client.get_object(Bucket=bucket_name, Key=list_of_shards[0])
    final_doc = pd.read_csv((first_object_response.get("Body")))
    count_2 = 1
    length=len(list_of_shards)
    
    # df = pd.concat(
    # map(pd.read_csv, ['mydata.csv', 'mydata1.csv']), ignore_index=True)
    # print(df)

    #Write a new file (final_doc) by iterating through the list of files in the bucekt
    for i in list_of_shards[1:length]:
        response_2 = s3_client.get_object(Bucket=bucket_name, Key=i)
        data_1 = pd.read_csv((response_2.get("Body")))
        data_1 = data_1.dropna(thresh=2)
        #print(titanic_data)
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