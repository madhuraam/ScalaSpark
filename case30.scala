package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object case30 {
  
  def main(args : Array[String]) {
      
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc= new SparkContext("local[*]","case30")
    
    val empName = sc.textFile("../files/EmpName.csv").map(x => (x.split(",") (0),x.split(",") (1)) )
    
    val empSal = sc.textFile("../files/EmpSalary.csv").map(x => (x.split(",") (0),x.split(",") (1)) )
    
    val empMgr = sc.textFile("../files/EmpManager.csv").map(x => (x.split(",") (0),x.split(",") (1)) )
    
    val empNamSal = empName.join(empSal).join(empMgr).map(x => (x._1, x._2))
    
    val joined = empNamSal.sortByKey()
    
    val result = joined.map( v => (v._1, v._2._1._1,v._2._1._2,v._2._2 ))
    
    result.collect.foreach(println)
    
    //empNamSal.repartition(1).saveAsTextFile("../files/case30")
  }
}