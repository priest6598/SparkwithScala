import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaTrendyEx1 {
    def main(args:Array[String]) = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("Count Chapter in Courses").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val chaptersInput = sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/chapters.csv")
      val finalRdd = chaptersInput.map(x=>(x.split(",")(1),x.split(",")(0)))
      val myRes = finalRdd.map(x=>(x._1,1))
      val finalRDD1 = myRes.reduceByKey((x,y)=>x+y)
      val finalR = finalRDD1.sortBy(x=>x._2,false)
      finalR.collect.foreach(println)
    }
}
