/*This is a Demo code used for line counts in a given document
*/
package a_z.in.hadoop.Maven_Example1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Line_Count{
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Spark\\")

    val logFile = "D:\\Softwares\\Spark_Binary\\spark-1.3.1-bin-hadoop2.4\\README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))  
  }
}
