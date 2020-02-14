import os


def cp_mv_localidades():
        os.system('sh script_localidades.sh')

        #output = open('localidades.txt', 'r').read()
        with open("localidades.txt") as f:
                lines = [line.strip() for line in f if line.strip()]
        #print(lines)
        #print(lines[0])
        output = lines[0]

        os.system("aws s3 cp s3://emr-job-spark/localidades/"+output+" s3://emr-job-spark/localidades/localidades.csv")
        os.system("aws s3 mv s3://emr-job-spark/localidades/"+output+" s3://emr-job-spark/localidades/old/")