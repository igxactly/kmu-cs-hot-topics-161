/* Ingu Kang
 * Kookmin Univ.
 *
 * References:
 * FPGrowthExample.scala in https://github.com/apache/spark
 */

/* TODO: use hdfs for input/output */
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.fpm.FPGrowth

object FPGrowthPractice {
  case class Params(
    input: String = "file:///home/cloudera/Downloads/webdocs.dat",
    minSupport: Double = 0.3,
    numPartition: Int = 5)

  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val defaultParams = Params()

    run(defaultParams)
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"FPGrowth Practice")
    val sc = new SparkContext(conf)

    /* split each transactions(lines) into an array of words by spaces */
    val transactions = sc.textFile(params.input).map(_.split(" ")).cache()

    println(s"Number of transactions: ${transactions.count()}")

    /* set a FPGrowth model with minSupport and the number of partitions */
    val model = new FPGrowth()
      .setMinSupport(params.minSupport)
      .setNumPartitions(params.numPartition)
      .run(transactions) /* then hand over the transactions */

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    sc.stop()
  }
}
