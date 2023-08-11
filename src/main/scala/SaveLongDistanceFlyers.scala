import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.kafka.streams.StreamsConfig

import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.time.Duration
import java.util.Collections
import org.apache.kafka.clients.consumer.ConsumerRecords


import java.util.concurrent.CountDownLatch
import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`}

object SaveLongDistanceFlyers extends App{


  Class.forName("org.postgresql.Driver")
  val dbConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "abc123")

  val props: Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)
  props.setProperty("enable.auto.commit", "true")
  props.setProperty("auto.commit.interval.ms", "1000")

  val consumer = new KafkaConsumer[String, Long](props)

  val topic = "long-distance-travellers"
  consumer.subscribe(Collections.singletonList(topic))



  def processRecords(records: ConsumerRecords[String, Long], dbConn: Connection, latch: CountDownLatch): Unit = {
    val deleStmt = dbConn.prepareStatement("DROP TABLE IF EXISTS longdistancetravellers")
    deleStmt.executeUpdate()

    val createStmt = dbConn.prepareStatement("""CREATE TABLE longdistancetravellers (
                                               |  id SERIAL PRIMARY KEY,
                                               |  bee_id VARCHAR(255) NOT NULL,
                                               |  square_count bigint
                                               |);""".stripMargin)
    createStmt.executeUpdate()
      if (records.isEmpty) {
      } else {

        records.forEach(record => {
          val beeId = record.key()
          val count = record.value()
          if(! beeId.isEmpty && !count.isNaN  )
            {
              val addStmt = dbConn.prepareStatement("INSERT INTO longdistancetravellers (bee_id, square_count) VALUES (?, ?) ON CONFLICT DO NOTHING")
              addStmt.setString(1, beeId)
              addStmt.setLong(2, count)
              addStmt.execute()
              println(s"Saved long-distance traveller $beeId with $count unique squares visited to the database")
            }
          latch.countDown()

        })
      }
    }



    val records = consumer.poll(Duration.ofMillis(100000))
    val latch = new CountDownLatch(records.size)
  while (latch.getCount > 0) {
    processRecords(records, dbConn,new CountDownLatch(records.size))
  }


    sys.ShutdownHookThread {
      consumer.close()
      dbConn.close()
    }

}