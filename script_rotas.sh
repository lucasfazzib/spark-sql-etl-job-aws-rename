#!bin/bash

for key in `aws s3 ls s3://emr-job-spark/rotas/ | awk '{print $4}' | grep part`
do
        `aws s3 ls s3://emr-job-spark/rotas/ | awk '{print $4}' | grep part > rotas.txt`
        #echo $key
done