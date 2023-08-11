import GenerateBeeFlight.groupSize

import java.util.UUID
import java.util.Properties
import scala.util.Random
import java.time.Instant
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

object GenerateBeeFlight extends App {
  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put("acks", "all")
  props.put("linger.ms", 1)
  props.put("retries", 0)

  val producer: Producer[String, String] = new KafkaProducer[String, String](props)

  val W: Int = 10 // Width of the area
  val H: Int = 10 // Height of the area
  val minInterval: Int = 1 // Minimum time interval between events (in seconds)
  val maxInterval: Int = 10 // Maximum time interval between events (in seconds)
  val groupSize: Int = 10

  // Generate and publish bee landing events
  def generateAndPublishEvents(): Unit = {
    generateAndPublishEvent(producer, W, H, minInterval, maxInterval, groupSize)
    generateAndPublishEvents()
  }





  def generateAndPublishEvent(producer: Producer[String, String], W: Int, H: Int, minInterval: Int, maxInterval: Int,groupSize: Int): Unit = {
    val idGroup: List[UUID] = List.fill(groupSize)(UUID.randomUUID())
    val beeId: String = idGroup(Random.nextInt(groupSize)).toString
    val timestamp = Instant.now().getEpochSecond
    val x = Random.nextInt(W)
    val y = Random.nextInt(H)

    val event = s"$beeId,$timestamp,$x,$y"
    println("event:", event)
    producer.send(new ProducerRecord[String, String]("events", null, event))
    val nextInterval = Random.nextInt(maxInterval - minInterval) + minInterval
    Thread.sleep(nextInterval * 10)
  }

  generateAndPublishEvents()

  sys.ShutdownHookThread {
    producer.flush()
    producer.close()
  }
}
