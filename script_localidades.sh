#!bin/bash

for key in `aws s3 ls s3://emr-job-spark/localidades/ | awk '{print $4}' | grep part`
do
        `aws s3 ls s3://emr-job-spark/localidades/ | awk '{print $4}' | grep part > localidades.txt`
        #echo $key
done
