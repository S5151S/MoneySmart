package com.org.ms

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object HomeTest {

  def setupStreamPipeline(
    topic:      String,
    brokerlist: String,
    freq:       Int)(): StreamingContext = {
     
    val spark = SparkSession.builder.master("local[*]").appName("MoneySmart").getOrCreate()
     val streaming = new StreamingContext(spark.sparkContext, Seconds(freq))
  
     val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokerlist, "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], "auto.offset.reset" -> "latest", "enable.auto.commit" -> (false: java.lang.Boolean), "group.id" -> "grp1")
    val stream = KafkaUtils.createDirectStream[String, String](
      streaming,
      PreferConsistent,
      Subscribe[String, String](Set(topic), kafkaParams))
      
    
    import spark.implicits._
      
val customSchema = StructType(Seq(StructField("year", IntegerType, true),StructField("make", StringType, true),StructField("model", StringType, true),StructField("comment", StringType, true),StructField("blank", StringType, true)))

  val schema=StructType(Seq(StructField("city",StringType, true),
                        StructField("country",StringType, true),
                        StructField("current_url",StringType, true),
                        StructField("date",StringType, true),
                        StructField("event",StringType, true),                        
                        StructField("init_session",StringType, true)))
      
    stream.foreachRDD { rdd =>   
     val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
     var x=rdd.map(row=>row.value())
  
     //SQLContext.createDataFrame(rdd, schema)
        
     

    }

    streaming
  }

  def main(args: Array[String]) {
    println("Hello")
    val nargs = 3
    if (args.length < nargs) {
      System.exit(1)
    }
    val Array(brokerlist, topic, frequency) = args

    val freq = frequency.toInt

    val streaming = setupStreamPipeline(topic, brokerlist, freq)
    streaming.start()
    streaming.awaitTermination()

  }

}
