import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: spark.example.WordCount <input> <output>")
      System.exit(1)
    }

    val input_path = args(0).toString
    val output_path = args(1).toString

    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(input_path)
    val countResult = inputFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(output_path)
  }
}