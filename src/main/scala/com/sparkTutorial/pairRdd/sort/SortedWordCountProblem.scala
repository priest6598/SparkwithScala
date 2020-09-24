package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    val conf = new SparkConf().setAppName("sorting-wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile("in/word_count.text")
    val pairRDD = inputRDD.flatMap(lines => lines.split(" "))
    val pairedRDD = pairRDD.map(word => (word, 1))
    val countToWordParis = pairedRDD.reduceByKey((x, y) => x + y)
    val sortedCountToWordParis = countToWordParis.sortBy(wordcount => wordcount._2, ascending = false)
    for((word, count) <- sortedCountToWordParis.collect()) println(word + ":" + count)
  }

}

