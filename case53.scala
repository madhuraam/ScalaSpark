package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object case53 {
  
   def main(args: Array[String]) {
    
     Logger.getLogger("org").setLevel(Level.ERROR)
     
     val sc = new SparkContext("local[*]","case53")
     //
     
     val a = sc.parallelize( 1 to 10, 3) 
     val b = a.filter { case x => (x% 2 == 0) }
     
     val c = a.filter( _ < 4)
     c.collect().foreach(println)
     
   }
}