package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object case31 {
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    val sc = new SparkContext("local[*]","case31")
    
    val contents = sc.textFile("../files/content.txt").flatMap(x => x.split(" ")).map( w => w.trim())
    
    val remove = sc.textFile("../files/remove.txt").flatMap(x => x.split(",")).map(w => w.trim())
       
    val bc = sc.broadcast(remove.collect().toList)
       
    val filt = contents.filter { case (w ) => !bc.value.contains(w) }
    
    val filtered = contents.subtract(remove)
      
    val wordcnt = filtered.map(x => (x, 1)).reduceByKey(_+_)
    
    filt.collect().foreach(println)
  }
}