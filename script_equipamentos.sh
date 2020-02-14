#!bin/bash

for key in `aws s3 ls s3://emr-job-spark/equipamentos/ | awk '{print $4}' | grep part`
do
        `aws s3 ls s3://emr-job-spark/equipamentos/ | awk '{print $4}' | grep part > equipamentos.txt`
        #echo $key
done
