package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Case1   {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Case1")
    
    val lines = sc.textFile("../user.csv")
    
    val header = lines.first()//lines.map(x => x.split(",")(1))
    
    val rdd= lines.filter(r => r != header )
    
    val ffr = rdd.filter( _ != "myself,cca175,180" ) 
    
    //val nRDD = sc.makeRDD(ffr.map( id -> "Rahul"))
    
    ffr.collect().foreach(println)
    
    val mapRRD = sc.parallelize(Seq(Map("id" -> "Rahul","topics" -> "scala", "hits" -> "120")))
    
    mapRRD.collect().foreach(println)
  }
}