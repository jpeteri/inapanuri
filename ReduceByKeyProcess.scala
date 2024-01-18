package com.ms.cddr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import collection.mutable.Map

object ReduceByKeyProcess {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ReduceByKeyProcess")
    val ssc = new SparkContext(sparkConf)
    val data = """
                      Bob,123,USA,Pizza,Soda,Biriyani,Blue
                      Bob,456,UK,Chocolate,Cheese,Soda,Green
                      Bob,12,USA,Chocolate,Pizza,Soda,Yellow
                      Mary,68,USA,Chips,Pasta,Chocolate,Blue
                      Bob,123,SG,Papad,Sambar,Rice,Purple
                      Bob,123,IN,Vadiams,Curry,VegRice,Grey
                      Mary,68,IN,Chips,Pasta,Chocolate,Blue

                """.trim

    val records = ssc.parallelize(data.split('\n'))
    val r2 = records.flatMap { r =>
      val Array(name, id, country, food1, food2, food3, color) = r.trim.split(',');
      List(((name, food1), 1), ((name, food2), 1), ((name, food3), 1))
      //List((id,Map("Name"->name,"Country"->country,"Food-Choice-1"->food1,"Food-Choice-2"->food2,"Food-Choice-3"->food3,"FavColor"->color)))
    }
    r2.foreach(x => println(x))
  }
}