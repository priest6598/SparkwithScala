package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */
    val conf = new SparkConf().setAppName("Longitude").setMaster("local[*]")
    val sc = new SparkContext(conf);
    val findinput = sc.textFile("in/airports.text")
    val findResultingRows = findinput.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40 )
    val ansRows = findResultingRows.map(line => {

      val answ = line.split(Utils.COMMA_DELIMITER)
      answ(1) + "," + answ(6)

    })
    ansRows.saveAsTextFile("out/latitude_Airport")
  }
}
