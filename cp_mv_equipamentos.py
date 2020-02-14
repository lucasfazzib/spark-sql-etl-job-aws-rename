import os

def cp_mv_equipamentos():
        os.system('sh script_equipamentos.sh')

        #output = open('equipamentos.txt', 'r').read()
        with open("equipamentos.txt") as f:
                lines = [line.strip() for line in f if line.strip()]
        #print(lines)
        #print(lines[0])
        output = lines[0]

        os.system("aws s3 cp s3://emr-job-spark/equipamentos/"+output+" s3://emr-job-spark/equipamentos/equipamentos.csv")
        os.system("aws s3 mv s3://emr-job-spark/equipamentos/"+output+" s3://emr-job-spark/equipamentos/old/")