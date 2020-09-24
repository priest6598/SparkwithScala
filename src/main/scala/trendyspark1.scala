import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object trendyspark1 {
  def main(args : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("AddYorN").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/dataset1")
    val impoColumn = inputRdd.map(x=> if(x.split(",")(1).toInt >18) (x,'Y')
    else (x,'N')
    )
    impoColumn.collect().foreach(println)
    //val result
    /*for(result <- impoColumn ){
      if(result>18) {
        println(result, 'Y')
      }
      else{
        println(result,'N')
        }

    }*/
  }
}
