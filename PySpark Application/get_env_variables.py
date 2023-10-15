import os

os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'root'
os.environ['password'] = '00000000'

header = os.environ['header']
envn = os.environ['envn']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']
appName = 'Pyspark Project'

current = os.getcwd()

src_olap = current + '\Source\olap'
src_oltp = current + '\Source\oltp'

city_path = 'output\cities'
presc_path = 'output\prescriber'
