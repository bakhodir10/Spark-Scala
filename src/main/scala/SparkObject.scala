
import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object SparkObject {

  // main method
  def main(args: Array[String]): Unit = {

    // spark config
    val sparkConf = new SparkConf().setAppName("Spark Object")
      .setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(sparkConf)

    // Step 2

    // create RDD
    val cane = sc.textFile("cane.csv") // creating RDD
    // a file is to write the result into
    val writer = new PrintWriter(new File("result.txt"))

    val rows = cane.map(line => line.split(",")) // making an array
    // first row in the array
    val header = rows.first()

    // Step 3

    // get list of all category and convert to RowNumAndCategory that has category name and value
    val population = rows.filter(line => !(line sameElements header))
      .map(line => RowNumAndCategory(line(5).substring(1, line(5).length - 1), line(1).toDouble))

    // Step 4

    // get list of values in category A
    val categoryA = population.filter(x => x.category.equals("A")).map(x => x.value)
    // get list of values in category B
    val categoryB = population.filter(x => x.category.equals("B")).map(x => x.value)
    // get list of values in category C
    val categoryC = population.filter(x => x.category.equals("C")).map(x => x.value)
    // get list of values in category D
    val categoryD = population.filter(x => x.category.equals("D")).map(x => x.value)

    val meanA: Double = categoryA.sum() // get mean of A
    val meanB = categoryB.sum() // get mean of B
    val meanC = categoryC.sum() // get mean of C
    val meanD = categoryD.sum() // get mean of D

    val sumA = categoryA.map(x => Math.pow(x - meanA, 2)).sum() // sum of numbers in A
    val countA = categoryA.map(x => Math.pow(x - meanA, 2)).count() // count of numbers in A
    val varA: Double = sumA / countA // variance of category A

    val sumB = categoryB.map(x => Math.pow(x - meanB, 2)).sum() // sum of numbers in B
    val countB = categoryB.map(x => Math.pow(x - meanB, 2)).count() // count of numbers in A
    var varB: Double = sumB / countB // variance of category B

    val sumC = categoryC.map(x => Math.pow(x - meanC, 2)).sum() // sum of numbers in C
    val countC = categoryC.map(x => Math.pow(x - meanC, 2)).count() // count of numbers in C
    val varC: Double = sumC / countC // variance of category C

    val sumD = categoryD.map(x => Math.pow(x - meanD, 2)).sum() // sum of numbers in D
    val countD = categoryD.map(x => Math.pow(x - meanD, 2)).count() // count of numbers in D
    val varD: Double = sumD / countD // variance of category D

    // write to the file
    writer.write("Category    Mean    Variance \n")
    writer.write("A          " + "%.2f".format(meanA) + "  %.2f".format(varA) + "\n")
    writer.write("B          " + "%.2f".format(meanB) + "  %.2f".format(varB) + "\n")
    writer.write("C          " + "%.2f".format(meanC) + "  %.2f".format(varC) + "\n")
    writer.write("D          " + "%.2f".format(meanD) + "  %.2f".format(varD) + "\n")
    writer.write("\n")

    // Step 4

    // get sample of the data with 0.25, no replacement
    val samplePopulation = population.sample(withReplacement = false, 0.25)

    // Step 5

    var mean1000A: Double = 0
    var var1000A: Double = 0

    var mean1000B: Double = 0
    var var1000B: Double = 0

    var mean1000C: Double = 0
    var var1000C: Double = 0

    var mean1000D: Double = 0
    var var1000D: Double = 0

    for (i <- 1 to 10) {

      // Step 5a

      // get sample of the data with 1, with replacement
      val resampleData = samplePopulation.sample(withReplacement = true, 1)

      // Step 5b, 5c

      // get list of values in category A
      val categoryA = resampleData.filter(x => x.category.equals("A")).map(x => x.value)
      // get list of values in category B
      val categoryB = resampleData.filter(x => x.category.equals("B")).map(x => x.value)
      // get list of values in category C
      val categoryC = resampleData.filter(x => x.category.equals("C")).map(x => x.value)
      // get list of values in category D
      val categoryD = resampleData.filter(x => x.category.equals("D")).map(x => x.value)

      val meanA = categoryA.sum() // get mean of A
      val meanB = categoryB.sum() // get mean of B
      val meanC = categoryC.sum() // get mean of C
      val meanD = categoryD.sum() // get mean of D

      val sumA = categoryA.map(x => Math.pow(x - meanA, 2)).sum()
      val countA = categoryA.map(x => Math.pow(x - meanA, 2)).count()
      val varA = sumA / countA
      var1000A += varA
      mean1000A += meanA

      val sumB = categoryB.map(x => Math.pow(x - meanA, 2)).sum()
      val countB = categoryB.map(x => Math.pow(x - meanA, 2)).count()
      val varB = sumB / countB
      var1000B += varB
      mean1000B += meanB

      val sumC = categoryC.map(x => Math.pow(x - meanC, 2)).sum()
      val countC = categoryC.map(x => Math.pow(x - meanC, 2)).count()
      val varC = sumC / countC
      var1000C += varC
      mean1000C += meanC

      val sumD = categoryD.map(x => Math.pow(x - meanD, 2)).sum()
      val countD = categoryD.map(x => Math.pow(x - meanD, 2)).count()
      val varD = sumD / countD
      var1000D += varD
      mean1000D += meanD
    }

    // Step 6

    // write to the file
    writer.write("Category    Mean    Variance \n")
    writer.write("A          " + "%.2f".format(mean1000A / 10) + "  %.2f".format(var1000A / 10) + "\n")
    writer.write("B          " + "%.2f".format(mean1000B / 10) + "  %.2f".format(var1000B / 10) + "\n")
    writer.write("C          " + "%.2f".format(mean1000C / 10) + "  %.2f".format(var1000C / 10) + "\n")
    writer.write("D          " + "%.2f".format(mean1000D / 10) + "  %.2f".format(var1000D / 10) + "\n")

    writer.close()
  }
}
