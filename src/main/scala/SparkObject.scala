
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}


object SparkObject {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Spark Object")
      .setMaster("local[2]").set("spark.executor.memory", "1g")

    val sc = new SparkContext(sparkConf)

    val tf = sc.textFile("cane.csv")
    val writer = new PrintWriter(new File("result.txt" ))

    val countsOne = tf
      .flatMap(line => line.split("\\s+"))
      .map(x => (x.toLowerCase(), 1))
      .reduceByKey(_ + _)
      .cache()
    val countsTwo = countsOne.sortByKey(true)
    countsTwo.foreach(println)

    val res = countsTwo.collect()
    for (n <- res) writer.println(n.toString())
    writer.close()
  }
}
