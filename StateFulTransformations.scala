package com.ms.cddr
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StateFulTransformations {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StateFulTransformations")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("C:\\xml\\checkpoint")

    // Create a ReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (e.g. generated by 'nc')
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // Update the cumulative count using mapWithState
    // This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(StateSpec.function(mappingFunc))
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}