import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaTrendyEx2 {
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Count Chapter in Courses").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val chaptersInput = sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/views-1.csv,C:/Users/Inspiran/Desktop/Dataforhadoop/views-2.csv,C:/Users/Inspiran/Desktop/Dataforhadoop/views-3.csv")
    val chapterRdd = sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/chapters.csv")
    val titleRdd =  sc.textFile("C:/Users/Inspiran/Desktop/Dataforhadoop/titles.csv")
    val titleRdd1 = titleRdd.map(x=>(x.split(",")(0),x.split(",")(1)))
    val chapterRdd1 = chapterRdd.map(x=>(x.split(",")(0),x.split(",")(1)))
    val chapterRdd2 = chapterRdd.map(x=>(x.split(",")(1),x.split(",")(0)))
    val userChapter = chaptersInput.map(x=>(x.split(",")(1),x.split(",")(0)))
    val distinctRdd = userChapter.distinct()
    //distinctRdd.take(10).foreach(println)
    val CourseChapter = distinctRdd.join(chapterRdd1).map(x=>(x._2,1))
    val countChapter = CourseChapter.reduceByKey((x,y)=>(x+y))
    val reduceRdd = countChapter.map(x=>(x._1._2,x._2))
    val myRes = chapterRdd2.map(x=>(x._1,1))
    val finalRDD1 = myRes.reduceByKey((x,y)=>x+y)
    val resRdd3 = finalRDD1.join(reduceRdd)
    val resRdd4 = resRdd3.mapValues(x=>{
      var p = (x._2.toFloat/x._1)*100
     // println(p)
      if(p>=90) {
       // println("hello")
        10

      }
      else if(p>50 && p < 90){
       // println("hello")
        4
      }
      else if(p>25 && p <=25) {
        2
      } else {
        0
      }
    })
    val resRdd5 = resRdd4.reduceByKey((x,y)=>x+y)
    val resRdd6 = resRdd5.join(titleRdd1).map(x=>(x._2._1,x._2._2))
    val resRdd7 = resRdd6.sortBy(x=>x._1,false)
    resRdd7.collect.foreach(println)

  }
}
