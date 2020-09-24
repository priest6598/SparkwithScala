import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object TrendytechMintemp {
  def main(args : Array[String]): Unit = {

    def localData(): Set[String]= {
      var localDataIs: Set[String]  = Set()
      val lines = Source.fromFile("C:/Users/Inspiran/Desktop/Dataforhadoop/boringwords.txt").getLines()
      for(line <- lines){
        localDataIs += line
      }
      localDataIs
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("AddYorN").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val bd = sc.broadcast(localData)
    val inputRdd = sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/bigdata-campaign-data.csv")
    val filterColumns = inputRdd.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
    val res = filterColumns.flatMapValues(x=>x.split(" "))
    val midRdd = res.map(x=> (x._2.toLowerCase(),x._1))
    val filterRDD = midRdd.filter(x=> !bd.value(x._1) )
    val result = filterRDD.reduceByKey((x,y)=> x+y)
    val sortres = result.sortBy(x=> x._2,false)
    sortres.take(20).foreach(println)
    //val result1 = filterColumns.reduceByKey((x,y)=>min(x,y))
  }
}
