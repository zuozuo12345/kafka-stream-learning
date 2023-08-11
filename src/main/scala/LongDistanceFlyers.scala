import GenerateBeeFlight.W
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}

import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.kstream.{Consumed, TimeWindows}

object LongDistanceFlyers extends App{

    import Serdes._

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

    class SetIntSerializer extends Serializer[Set[Int]] {
      override def serialize(topic: String, data: Set[Int]): Array[Byte] = {
        data.mkString(",").getBytes("UTF-8")
      }
    }

    class SetIntDeserializer extends Deserializer[Set[Int]] {
      override def deserialize(topic: String, data: Array[Byte]): Set[Int] = {
        new String(data, "UTF-8").split(",").map(_.toInt).toSet
      }
    }

    class SetIntSerde extends Serde[Set[Int]] {
      def deserializer(): Deserializer[Set[Int]] = new SetIntDeserializer

      def serializer(): Serializer[Set[Int]] = new SetIntSerializer
    }

    object SetIntSerde {
      def apply(): Serde[Set[Int]] = new SetIntSerde
    }



    def makeTopology(inputTopic: String, outputTopic: String) = {
////////////////////////////////////////////////////////////////////////////
      val K: Int = 2 // Threshold for long-distance travellers
      val W: Int = 10

      val builder = new StreamsBuilder

      val events: KStream[String, String] = builder.stream[String, String](inputTopic)(Consumed.`with`(stringSerde, stringSerde))

      var RepeatedID: Set[String] = Set()

      // Parse CSV events and map them to (beeId, squareId) pairs
      val beeAndSquareIdStream: KStream[String, Int] = events.map { (_, event) =>
        val Array(beeId, _, x, y) = event.split(",")
        val squareId = x.toInt + y.toInt * W
        println("all:",(beeId, squareId))
        (beeId, squareId)
      }

      // Count unique squares visited by each bee
      val materializedSetInt: Materialized[String, Set[Int], ByteArrayKeyValueStore] =
        Materialized.`with`[String, Set[Int], ByteArrayKeyValueStore](stringSerde, SetIntSerde())

      val uniqueSquaresVisited: KTable[String, Long] = beeAndSquareIdStream
        .groupByKey
        .aggregate[Set[Int]](Set.empty[Int])(
          (aggKey: String, newSquare: Int, squares: Set[Int]) => squares + newSquare
        )(materializedSetInt)
        .mapValues(squares =>
          squares.size)


      // Filter the long-distance travellers
      val longDistanceTravellers: KTable[String, Long] = uniqueSquaresVisited
        .filter((beeId, count) => {
          if (count == K + 1 && !RepeatedID.contains(beeId)) {
            RepeatedID += beeId
            true
          } else {
            false
          }
        })


      // Output the long-distance travellers to the 'long-distance-travellers' topic
      longDistanceTravellers.toStream.to(outputTopic)
      builder.build()
    }


    val streams: KafkaStreams = new KafkaStreams(makeTopology("events", "long-distance-travellers"), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(1000))
    }

}
