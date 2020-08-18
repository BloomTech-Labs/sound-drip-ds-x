# Discogs Lambda Function
> AWS Lambda Function to parse new \*masters.gz files from discogs s3 and upload parsed to account s3

![](https://github.com/Lambda-School-Labs/sound-drip-ds-x/blob/master/data_collection/discogs_lambda_function/function_process.jpg)

### Function Details
    - name: dscg-func
    - runtime: python 3.8
    - handler : lambda_handler
    - memory : 2048 MB
    - libraries : [boto3, datetime, gzip, json, os, xml.sax]
    

### Scheduled event trigger
    - Rule name: on_first_every_month_at_midnight
    - cron pattern: cron(0 0 1 * ? *)
    
 
