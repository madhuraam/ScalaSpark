# define the database
dbname="hive_dbase"

#loop the tables 
while read line
do
#get column names from the table
var=$(hive -e 'use '$dbname'; SHOW COLUMNS IN '$line)
wait

#define variables
DESTINATIONS=""
QUERY3=""

#form the query for columns 
for i in $var 
 do 
   DESTINATIONS="$DESTINATIONS \"$i\" , $i,"
   QUERY3="$QUERY3 SUM(IF( $i is NULL,1,0)) as $i,"  
 done

   #remove the last comma
   DESTINATIONS=${DESTINATIONS:0:$#-1}
   QUERY3=${QUERY3:0:$#-1}

 #form the query from loop variables
 QUERY33="Select  "$QUERY3" from "$line  

 QUERY2="Select map("$DESTINATIONS") as tmp_column from (" 
 QUERY2="$QUERY2 $QUERY33 ) a"
 
 QUERY1="Select \"$line\", srv , colName From ("$QUERY2") b lateral view explode(tmp_column) expl as srv, colName where colName<>0 "
 
 eval "hive -e 'use $dbname; $QUERY1'"
done