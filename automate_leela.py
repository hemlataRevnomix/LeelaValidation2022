# Import the required libraries
import os
from datetime import datetime,timedelta,date
# Define the location of the directory
today1 = date.today()
d1 = today1.strftime("%Y-%m-%d")

xDate = d1
path = r"C:\Chakradhar\hemlata\Validation project\leela\Validation Reports/"+d1

# Change the directory
os.chdir(path)

def read_files(file_path):
   with open(file_path, 'r') as file:
      with open("out.txt", 'a') as file2:
         data = file.readlines()
         for line in data:
            if 'NOT in pipe Separated' in line:
               file2.write(line+'\n')
            elif 'All Columns are  NOT' in line:
               file2.write(line[0:68]+'\t'+'filename:'+line[86:]+'\n')
            else:
               pass

               x = line.rsplit('\ ')

               # print(x)

         # print(file.read())

# Iterate over all the files in the directory

for file in os.listdir():
   if file.endswith('.txt'):
      # Create the filepath of particular file
      file_path =f"{path}/{file}"

      read_files(file_path)





