import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Producer {
  def main(args: Array[String]): Unit = {
    
        //build the spark config
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BatchFileProcessing")
    
        //create the spark Session
        val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    
    
        //the path of file to push the data from
        val filePath = "/Users/kodandaravikiranputta/dbms_project4/datastream2/src/main/resources/data.txt"


       //build the dataframe from the text file
        val textDF = spark.read.text(filePath)

        //convert the dataframe to RDD
        val processedData = textDF.rdd.map(row => row.getString(0))



        //publish the data in each partition to Kafka Topic.
        processedData.foreachPartition { partition =>
          import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
          val producerProperties = new java.util.Properties()

          //build the producer config
          producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
          producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") // Specify key serializer
          producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") // Specify value serializer

          val producer = new KafkaProducer[String, String](producerProperties)

          partition.foreach { message =>
            val record = new ProducerRecord[String, String]("molecularData", message.split("\\s+")(4), message)
            producer.send(record)
          }

          producer.close()
        }


        //Stop the SparkSession
        spark.stop()

  }
}
