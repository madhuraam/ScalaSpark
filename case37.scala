package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object case37  {
  
  def parseLine(line: String) = {
    
    val fields = line.split("~")
    //println(fields)
    val date = fields(1)
    (date)
  }
  
  val regx1 = "\\w+\\s\\d{2},\\s\\d{4}".r
  val regx2 = "^[0-3]?[0-9].[0-3]?[0-9].(?:[0-9]{2})?[0-9]{2}$".r
  
  def parseFilter(date : String) = date match  {
      
    case reg1 => date + "good"
    case reg2 => date + "good"
    case _ => date + "bad"
  }
  
  def main(args :Array[String])  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","case37")
    
    val file1 = sc.textFile("../files/file37.csv")
    
    val date = file1.map(parseLine)
    
    //val valid = date.map(parseFilter)
    
    val validdate1 = date.filter( _.matches ("^[0-3]?[0-9].[0-3]?[0-9].(?:[0-9]{2})?[0-9]{2}$"))       
    
    val validdate2 = date.filter( _.matches ("\\w+\\s\\d{2},\\s\\d{4}"))
    
    val joindate = validdate1.union(validdate2)
    
    
    joindate.repartition(1).saveAsTextFile("../files/correctdates.txt")
    
    joindate.collect().foreach(println)    
   
    val intersection = date.subtract(joindate)
    
    intersection.repartition(1).saveAsTextFile("../files/incorrectdates.txt")
    
    intersection.collect().foreach(println)   
  }
}