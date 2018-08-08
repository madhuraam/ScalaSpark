# ScalaSpark

Case files for expriment;


Count tables shell file to get column names from all tables in a database if columns are null.
Please run the below to populate the tables from database to a file 
Provide the database name to the script

 hive -e 'use [database_name]; show tables' | tee tables.txt

Open the shell script update the database name to the variable 'dbname' and save.
Run the script in shell window 

chmod +x count_tables.sh

./count_tables.sh < tables.txt > counts.xlsx
