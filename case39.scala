package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object case39  {
  
  def parseLine(line: String) = {
    
    val fields = line.split(",")
    //println(fields(1), fields(2))
    (fields(0),fields(1), fields(2))
    
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]", "case39")
    
    val file1 = sc.textFile("../files/file1.txt")
    
    val file2 = sc.textFile("../files/file2.txt")
        
    val RDD1 = file1.map(parseLine).map( x => (x._1, (x._2, x._3) )) //file1.map(x => x.split(",")).map(fields => (fields(1),(fields(2), fields(3)))).collect()
                
    val RDD2 = file2.map(parseLine).map( x => (x._1, (x._2, x._3) ))
    
    //RDD2.collect().foreach(println)
    
    val join = RDD1.join(RDD2)//.mapValues((x,y) => x._1, (x._2, y._2) )
    
    join.collect().foreach(println)
    
    val sum = ((join.values).keys).values
      
    val result = sum.reduce(_ + _)
    
     println(result)
    
    
  }
  
}