import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator}

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.kstream.{Aggregator, Initializer, KeyValueMapper, Transformer, TransformerSupplier}

import org.apache.kafka.clients.producer.ProducerRecord





object CountBeeLanding extends App{
  import Serdes._



  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.intSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")







  def makeTopology(inputTopic: String, outputTopic: String) = {
    val T: Int = 3
    val W: Int = 10
    val H: Int = 10

    val builder = new StreamsBuilder

    val eventsStream = builder.stream[String, String](inputTopic)

    val squareAndBeeIdTimestampStream: KStream[Int, Long] = eventsStream.map { (_, event) =>
      val Array(beeId, timestamp, x, y) = event.split(",")
      val squareId = x.toInt + y.toInt * W
      (squareId, timestamp.toLong)
    }

    var timeStampVar: Long = 0


    var processorContext: ProcessorContext = null
    var squareIdCounts: Map[Int, Int] = (0 to W * H - 1).map(i => (i, 0)).toMap

    val test = Map("123" -> "12313", "456546"->"11")
    val outputStream: KStream[String, String] = squareAndBeeIdTimestampStream.transform(new TransformerSupplier[Int, Long, KeyValue[String, String]] {
      override def get(): Transformer[Int, Long, KeyValue[String, String]] = new Transformer[Int, Long, KeyValue[String, String]] {
        val flushLock = new AnyRef
        private var punctuatorCancellable: Cancellable = _

        override def init(context: ProcessorContext): Unit = {
          processorContext = context
          val flushIntervalMillis = 1000L // Adjust this value as needed
          val flushIntervalDuration = Duration.ofMillis(flushIntervalMillis)
          punctuatorCancellable = processorContext.schedule(flushIntervalDuration, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
            override def punctuate(timestamp: Long): Unit = {
              println("Flushing remaining counts")
              flushCounts()
            }
          })
        }

        var lastReceivedTimestamp = System.currentTimeMillis()
        val endOfStreamTimeout: Long = 26 // Adjust this value as needed


        override def transform(key: Int, value: Long): KeyValue[String, String] = {
          flushLock.synchronized {
            lastReceivedTimestamp = System.currentTimeMillis()
            // Update the lastReceivedTimestamp for each received message
            // Regular input processing
            if (timeStampVar == 0) timeStampVar = value
            if (value >= timeStampVar && value < timeStampVar + T) {
              squareIdCounts.get(key) match {
                case Some(count) =>
                  squareIdCounts = squareIdCounts + (key -> (count + 1))
              }
            } else if (value >= timeStampVar + T) {
              flushCounts()
              timeStampVar += T
              squareIdCounts = (0 to W * H - 1).map(i => (i, 0)).toMap
              squareIdCounts.get(key) match {
                case Some(count) =>
                  squareIdCounts = squareIdCounts + (key -> (count + 1))
              }
            } else if (System.currentTimeMillis() - lastReceivedTimestamp > endOfStreamTimeout) {
              flushCounts()
              return new KeyValue(key.toString, squareIdCounts(key).toString)
            }

            if (System.currentTimeMillis() - lastReceivedTimestamp > endOfStreamTimeout) {
              flushCounts()
              println("squareIdCounts", squareIdCounts)
            }
          }
          null
        }

        def flushCounts(): Unit = {
          flushLock.synchronized {
            squareIdCounts.foreach { case (squareId, count) =>
              processorContext.forward(squareId.toString, count.toString) // Forward the updated KeyValue
            }
          }
        }

        override def close(): Unit = {
          if (punctuatorCancellable != null) {
            // Check if the stream has ended before closing
            if (System.currentTimeMillis() - lastReceivedTimestamp > endOfStreamTimeout) {
              flushCounts()
              println("squareIdCounts", squareIdCounts)
            }
            punctuatorCancellable.cancel()
          }
        }
      }
    })
    //      .peek((key: String, value: String) => {
    //      processorContext.forward("123", "12345")
    //      squareIdCounts.foreach { case (key, value) =>
    //        println(s"key:${key.toString},value:${value.toString}")
    //        processorContext.forward(key.toString,value.toString) // Forward the updated KeyValue
    //      }
    //    })

    outputStream.to(outputTopic)

    builder.build()

  }

  val streams: KafkaStreams = new KafkaStreams(makeTopology("events", "bee-counts"), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(100000))
  }
}