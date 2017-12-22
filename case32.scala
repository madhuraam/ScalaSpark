package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object case32  {
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","case32")
    
    val files = sc.textFile("../files/case321.txt,../files/case322.txt,../files/case323.txt").flatMap( x => x.split(" ")).map(w => w.trim())        
    
    val remRDD = sc.parallelize(List("a","the","an","as","with","this","these","is","are","in","for","to","and","the","of"))
    
    val diff= files.subtract(remRDD)
    
    val wrd = diff.map( x => (x, 1)).reduceByKey(_+_)
    
    wrd.sortByKey()
    
    wrd.collect().foreach(println)
  }
}