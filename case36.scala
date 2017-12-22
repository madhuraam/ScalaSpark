package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object case36 {
  
  def parseLine(line : String ) = {
    
    val fields = line.split(",")
    (fields(0),(fields(1)))
    
  }
  
  def main(args : Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","case36")
    
    val file = sc.textFile("../files/file36.csv")
    
    val lines = file.map(parseLine)
    
    val addBraces = lines.map( x => (x._1, (x._2) ))
    
    addBraces .collect().foreach(println)
    
  }
  
}