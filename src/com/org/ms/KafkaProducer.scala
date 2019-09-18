
 import java.util.Properties
import scala.io.Source
 import org.apache.kafka.clients.producer._
 import java.sql.Timestamp
 
object KafkaProducer extends App {
  val  props = new Properties()
 props.put("bootstrap.servers", "localhost:9092")
  
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

 val producer = new KafkaProducer[String, String](props)
   
 val topic="test"
 val filename = "clickstream-sample.json"
  
for (line <- Source.fromFile(filename).getLines) {
   val record = new ProducerRecord(topic,java.util.UUID.randomUUID.toString, line)
  producer.send(record)
}  
 producer.close()
  
}