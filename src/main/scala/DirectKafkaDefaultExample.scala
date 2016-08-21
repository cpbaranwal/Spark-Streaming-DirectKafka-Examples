package main.scala

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils


object DirectKafkaDefaultExample {
   private val conf = ConfigFactory.load()
  private val sparkStreamingConf = conf.getStringList("DirectKafkaDefaultExample-List").asScala
  val logger = Logger.getLogger(DirectKafkaDefaultExample.getClass)
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.exit(1)
    }
    val Array(brokers, topics) = args
    val checkpointDir = "/tmp/checkpointLogs"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    // Extract : Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val ssc = StreamingContext.getOrCreate(checkpointDir, setupSsc(topicsSet, kafkaParams, checkpointDir) _)
    ssc.start()// Start the spark streaming
    ssc.awaitTermination();
  }
  def setupSsc(topicsSet:Set[String],kafkaParams:Map[String,String],checkpointDir:String)():StreamingContext=
  { //setting sparkConf with configurations
    val sparkConf = new SparkConf()
    sparkConf.setAppName(conf.getString("DirectKafkaDefaultExample"))
    sparkStreamingConf.foreach { x => val split = x.split("="); sparkConf.set(split(0), split(1));}
    val ssc = new StreamingContext(sc, Seconds(conf.getInt("application.sparkbatchinterval")))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val line = messages.map(_._2)
    val lines = line.flatMap(line => line.split("\n"))
    val filteredLines = lines.filter { x => LogFilter.filter(x, "1") }
    filteredLines.foreachRDD((rdd: RDD[String], time: Time) => {
      rdd.foreachPartition { partitionOfRecords => {
        if (partitionOfRecords.isEmpty) {
          logger.info("partitionOfRecords FOUND EMPTY ,IGNORING THIS PARTITION")
        } else {
          /* write computation logic here  */
        }
      } //partition ends
      }//foreachRDD ends
    })
    ssc.checkpoint(checkpointDir) // the offset ranges for the stream will be stored in the checkpoint
    ssc }
  
}