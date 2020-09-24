import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object pracwcspark {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("WOrdCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/search_data.txt")
    val rdd2 = rdd1.flatMap(x => x.split(" "))
    val rdd3 = rdd2.map(_.toLowerCase)
    val rdd4 = rdd3.map(x => (x, 1))
    val rdd5 = rdd4.reduceByKey((x, y) => x + y)
    val rdd6 = rdd5.collect
    val rdd7 = rdd6.sortBy(x=>x._2)
    for(results <- rdd7) {
      val word = results._1
      val count = results._2
      println(s"$word : $count")
    }



  }

}
