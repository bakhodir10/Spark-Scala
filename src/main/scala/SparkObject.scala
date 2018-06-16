import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object SparkObject {

  // main method
  def main(args: Array[String]): Unit = {

    // spark config
    val sparkConf = new SparkConf().setAppName("Spark Object")
      .setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(sparkConf)
    // create RDD
    val cane = sc.textFile("cane.csv") // creating RDD
    // a file is to write the result into
    val writer = new PrintWriter(new File("result.txt"))

    val rows = cane.map(line => line.split(",")) // making an array
    // first row in the array
    val header = rows.first()

    // get list of all category and convert to RowNumAndCategory that has category name and value
    val population = rows.filter(line => !(line sameElements header))
      .map(line => new RowNumAndCategory(line(5).substring(1, line(5).length - 1), line(1).toDouble))

    // get list of values in category A
    val categoryA = population.filter(x => x.category.equals("A")).map(x => x.value)
    // get list of values in category B
    val categoryB = population.filter(x => x.category.equals("B")).map(x => x.value)
    // get list of values in category C
    val categoryC = population.filter(x => x.category.equals("C")).map(x => x.value)
    // get list of values in category D
    val categoryD = population.filter(x => x.category.equals("D")).map(x => x.value)

    val meanA = categoryA.sum() // get mean of A
    val meanB = categoryB.sum() // get mean of B
    val meanC = categoryC.sum() // get mean of C
    val meanD = categoryD.sum() // get mean of D

    val sumA = categoryA.map(x => Math.pow(x - meanA, 2)).sum() // sum of numbers in A
    val countA = categoryA.map(x => Math.pow(x - meanA, 2)).count() // count of numbers in A
    val varA = sumA / countA // variance of category A

    val sumB = categoryB.map(x => Math.pow(x - meanB, 2)).sum() // sum of numbers in B
    val countB = categoryB.map(x => Math.pow(x - meanB, 2)).count() // count of numbers in A
    val varB = sumB / countB // variance of category B

    val sumC = categoryC.map(x => Math.pow(x - meanC, 2)).sum() // sum of numbers in C
    val countC = categoryC.map(x => Math.pow(x - meanC, 2)).count() // count of numbers in C
    val varC = sumC / countC // variance of category C

    val sumD = categoryD.map(x => Math.pow(x - meanD, 2)).sum() // sum of numbers in D
    val countD = categoryD.map(x => Math.pow(x - meanD, 2)).count() // count of numbers in D
    val varD = sumD / countD // variance of category D

    for (i <- 1 to 1000) {

    }
  }
}
