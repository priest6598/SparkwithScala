import org.apache.log4j.{Level, Logger}


import org.apache.spark.{SparkConf, SparkContext}

object listparaller {
   def main(args : Array[String])={
     Logger.getLogger("org").setLevel(Level.ERROR)
     val conf = new SparkConf().setAppName("Listparallelized").setMaster("local[*]")
     val sc = new SparkContext(conf)
     val listOfItems = List("Error: my name","Warn: my name is ","Error: Happy")
     val listToRdd = sc.parallelize(listOfItems)
     val finalArr = listToRdd.map(x=>{
       val items = x.split(":")
       (items(0),1)
     })

     val aggre = finalArr.reduceByKey((x,y)=>x+y)
     aggre.collect().foreach(println)
   }
}
