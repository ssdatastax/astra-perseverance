#!/bin/bash

set -x

SEGMENT_TOKEN=$SEGMENT_KEY
TOKEN=`echo ${SEGMENT_TOKEN}: | base64`


if [ -z "$BUCKET" ] || [ -z "$KEY" ] ||  [ -z "$EMAIL" ]  || [ -z "$ORIGINALNAME" ] || [ -z "$NOW" ]; then
  echo "the following ENV VARS are required BUCKET, KEY, EMAIL, NOW"
  echo "for example: $ docker run -e BUCKET=astra-sizer-diag -e KEY=diag-cluster-1.zip -e EMAIL=test@test.io  -e NOW=now --rm --name s3-diag phact/s3-diag"
  exit 1
fi

echo "$NOW"
#delete secrets because $$$
aws secretsmanager delete-secret --secret-id temp_"$EMAIl"_email_"$NOW" --force-delete-without-recovery --region us-east-1
aws secretsmanager delete-secret --secret-id temp_"$EMAIL"_bucket_"$NOW" --force-delete-without-recovery --region us-east-1
aws secretsmanager delete-secret --secret-id temp_"$EMAIl"_key_"$NOW" --force-delete-without-recovery --region us-east-1
aws secretsmanager delete-secret --secret-id temp_"$EMAIL"_"$NOW" --force-delete-without-recovery --region us-east-1

if [ ! -z "$EMAIL" ] ; then

    curl -X POST https://api.segment.io/v1/track -H 'Accept: */*'  \
     -H 'Accept-Encoding: gzip, deflate' \
     -H "Authorization: Basic $TOKEN" \
     -H 'Cache-Control: no-cache' \
     -H 'Connection: keep-alive' \
     -H 'Content-Type: application/json' \
     -H 'Host: api.segment.io' \
     -H 'cache-control: no-cache,no-cache' \
     -d "{ 
        \"userId\": \"$EMAIL\", 
        \"event\": \"Astra Sizer - Task Initiated\", 
        \"properties\": {  
            \"email\": \"$EMAIL\"
        }
    }"


fi

# encryption requires this signing version
aws configure set s3.signature_version s3v4

aws s3 cp s3://"$BUCKET"/"$KEY" "$KEY"

unzip "$KEY"

#  curl -i https://track.customer.io/api/v1/events \
#    -X POST \
#    -u "$CIO_SITE_ID":"$CIO_API_KEY" \
#    -d name=task_success \
#    -d data[dbid]="$DB_ID" \
#    -d data[table_name]="$TABLE" \
#    -d data[keyspace]="$KEYSPACE" \
#    -d data[org_id]="$ORG_ID" \
#    -d data[db_name]="$DB_NAME" \
#    -d data[recipient]="$EMAIL"

python explore.py -p ${ORIGINALNAME%.zip}

ls "${ORIGINALNAME%.zip}"

aws s3 cp "${ORIGINALNAME%.zip}/summary.json" s3://"$BUCKET"/"${KEY%.zip}-summary.json" 
aws s3 cp "${ORIGINALNAME%.zip}"/*.xlsx s3://"$BUCKET"/"${KEY%.zip}.xlsx" 

aws s3 rm s3://"$BUCKET"/"$KEY" 
