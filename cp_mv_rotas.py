import os

def cp_mv_rotas():
        os.system('sh script_rotas.sh')

        #output = open('rotas.txt', 'r').read()
        with open("rotas.txt") as f:
                lines = [line.strip() for line in f if line.strip()]
        #print(lines)
        #print(lines[0])
        output = lines[0]

        os.system("aws s3 cp s3://emr-job-spark/rotas/"+output+" s3://emr-job-spark/rotas/rotas.csv")
        os.system("aws s3 mv s3://emr-job-spark/rotas/"+output+" s3://emr-job-spark/rotas/old/")