# Discogs Lambda Function
> AWS lambda Function to copy new files from discgos s3 to our s3

### Function Details
    - name: dscg-func
    - runtime: python 3.8
    - handler : lambda_handler
    - memory : 128 MB
    - libraries : [json, boto3]
    

### Scheduled event trigger
    - Rule name: on_first_every_month_at_midnight
    - cron pattern: cron(0 0 1 * ? *)
    
 
