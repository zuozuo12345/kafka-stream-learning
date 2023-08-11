import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.util.{Collections, Properties}
import org.apache.kafka.clients.admin.{AdminClient, OffsetSpec}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import java.util.{Collections, Properties, Random, UUID}
import java.time.{Duration, Instant}
import org.apache.kafka.clients.producer.{KafkaProducer, MockProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver, _}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.DriverManager
import java.util.concurrent.CountDownLatch


class TestSolution extends AnyFunSuite {

  test("GenerateBeeFlight: bounds should be positive") {
    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)

    val W = 10
    val H = 10
    val minInterval = 1
    val maxInterval = 10
    val groupSize: Int = 10

    assert(W > 0, "Width (W) should be positive")
    assert(H > 0, "Height (H) should be positive")

    val idGroup: List[UUID] = List.fill(groupSize)(UUID.randomUUID())
    GenerateBeeFlight.generateAndPublishEvent(producer, W, H, minInterval, maxInterval,groupSize)

    assert(producer.history().size() == 1)
    val event = producer.history().get(0).value()
    val Array(beeId, timestamp, x, y) = event.split(",")

    assert(UUID.fromString(beeId) != null)
    assert(Instant.ofEpochSecond(timestamp.toLong) != null)
    assert(x.toInt >= 0 && x.toInt < W)
    assert(y.toInt >= 0 && y.toInt < H)
  }

  import Serdes._

  test("CountBeeLandings Test") {

    val props = new java.util.Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    props.put(StreamsConfig.STATE_DIR_CONFIG, "src/test/resources/test-state-store")

    val inputTopic = "events"
    val outputTopic = "bee-counts"
    val testDriver = new TopologyTestDriver(CountBeeLanding.makeTopology(inputTopic, outputTopic), props)

    val inputTestTopic: TestInputTopic[String, String] = testDriver.createInputTopic(inputTopic, stringSerde.serializer(), stringSerde.serializer())
    val outputTestTopic: TestOutputTopic[String, String] = testDriver.createOutputTopic(outputTopic, stringSerde.deserializer(), stringSerde.deserializer())


    ////////////////////////////////////////////////////////////////////////////
    val groupSize = 10
    val idGroup: List[UUID] = List.fill(groupSize)(UUID.randomUUID())
    val W: Int  = 10
    val H: Int  = 10
    val T: Int = 3
    val testRunTime = 4 * 1000

    var counts: Map[Int, Int] = (0 to W*H - 1).map(i => (i, 0)).toMap
    var actualMap: Map[Int, Int] = Map()
    var actual: List[(String, Int)] = List()
    var sortedList: List[(String, Int)] =  List()
    var lastTimestamp: Long = -1

    def generateRandomEvent(): (String, String) = {
      val beeId = idGroup(new Random().nextInt(groupSize)).toString
      val timestamp = Instant.now().getEpochSecond
      ////////////////////////////////////////////////////////////////////////////
      val x = new Random().nextInt(W)
      ////////////////////////////////////////////////////////////////////////////
      val y = new Random().nextInt(H);
      val squareId = x.toInt + y.toInt * W
      val event = s"$beeId,$timestamp,$x,$y"
      try {
        if (lastTimestamp < 0) {lastTimestamp = timestamp}
        if (timestamp - lastTimestamp >= T ) {
          lastTimestamp = timestamp
          var sortedCounts = counts.toList.sortBy(_._1).map { case (key, value) => (key.toString, value) }
          sortedList = sortedList ++ sortedCounts
          counts = (0 to 99).map(i => (i, 0)).toMap
        }
      }finally {
        counts = counts.updated(squareId, counts(squareId) + 1)
      }
      (beeId, event)
    }

    def runFunctionForSeconds(): Unit = {
      val startTime = System.currentTimeMillis
      while (System.currentTimeMillis() - startTime < testRunTime) {
        val event = generateRandomEvent()
        inputTestTopic.pipeInput(event._1, event._2)
      }
//      var sortedCounts = counts.toList.sortBy(_._1).map { case (key, value) => (key.toString, value) }
//      sortedList = sortedList ++ sortedCounts
      Thread.sleep(5000) // Sleep for 5 seconds
    }

    runFunctionForSeconds()
    Thread.sleep(5000)

    val results = outputTestTopic.readKeyValuesToList()

    results.forEach { result =>

      val (squareId, count) = (result.key, result.value)
      actualMap = actualMap + (squareId.toInt -> count.toInt)
      if(actualMap.size == W * H) {
        try {
          var actualCounts = actualMap.toList.sortBy(_._1).map { case (key, value) => (key.toString, value) }
          actual = actual ++ actualCounts
          //          Thread.sleep(4000)
        } finally {
          actualMap = Map()
        }
      }
    }



    println("______________________________")
    println("actuallist:",actual)
    println("sortedList:",sortedList)
    Thread.sleep(5000)

    assert(actual == sortedList)


    testDriver.close()
  }


  test("LongDistanceFlyers Test") {

    val props = new java.util.Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    props.put(StreamsConfig.STATE_DIR_CONFIG, "src/test/resources/test-state-store")


    val inputTopic = "events"
    val outputTopic = "long-distance-travellers"
    val testDriver = new TopologyTestDriver(LongDistanceFlyers.makeTopology(inputTopic, outputTopic), props)

    val inputTestTopic: TestInputTopic[String, String] = testDriver.createInputTopic(inputTopic, stringSerde.serializer(), stringSerde.serializer())
    val outputTestTopic: TestOutputTopic[String, Long] = testDriver.createOutputTopic(outputTopic, stringSerde.deserializer(), longSerde.deserializer())

    ////////////////////////////////////////////////////////////////////////////
    val groupSize = 10
    val idGroup: List[UUID] = List.fill(groupSize)(UUID.randomUUID())

    ////////////////////////////////////////////////////////////////////////////
    val K: Int = 2


    def generateRandomEvent: (String, String) = {
      val beeId = idGroup(new Random().nextInt(groupSize)).toString
      val timestamp = Instant.now().getEpochSecond
      ////////////////////////////////////////////////////////////////////////////
      val x = new Random().nextInt(10)
      ////////////////////////////////////////////////////////////////////////////
      val y = new Random().nextInt(10)
      val event = s"$beeId,$timestamp,$x,$y"
      println("all in test",(beeId, event))
      (beeId, event)
    }

    var result: Set[(String, String)] =Set()
    var counts: Map[String, Int] = Map().withDefaultValue(0)
    var excepted: Set[(String, Long)] =Set()

    def generateAndSendEvents(count: Int): Unit = {
      (1 to count).foreach { _ =>
        val event = generateRandomEvent
        val arr = event._2.split(",")
        val event2 = (event._1,arr.take(3).mkString(","))
        if (!result.contains(event2)) {
          counts = counts + (event._1 -> (counts(event._1) + 1))

          if (counts(event._1) == K + 1) {
            excepted += ((event._1, (K + 1).toLong))
            println("reach at K + 1",(event._1, counts(event._1)))
            counts -= event._1
          }
          inputTestTopic.pipeInput(event._1, event._2)
        }
      }
    }



    ////////////////////////////////////////////////////////////////////////////
    generateAndSendEvents(20)


    var actual : Set[(String,Long)] = Set()

    while (!outputTestTopic.isEmpty()) {
      val result = outputTestTopic.readKeyValue()
      if (result != null) {
        val (beeId, uniqueSquaresVisited) = (result.key, result.value)
        actual+= ((beeId, uniqueSquaresVisited))
      }
    }

    println("actual:",actual)


    actual shouldBe excepted


    testDriver.close()
  }

  test("saveLongDistanceFlyers Test") {

/*
* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
*
* IMPORTANT
*
*  To run last test, please first run
*
*   kafka-consumer-groups --bootstrap-server localhost:9092 --group test-group --topic long-distance-travellers --reset-offsets --to-latest --execute
*
* in kafka terminal in docker to clear offset before.
*
*
* !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
*
* */
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // Add this line
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(StreamsConfig.STATE_DIR_CONFIG, "src/test/resources/test-state-store")


    Class.forName("org.postgresql.Driver")
    val testdbConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "abc123")
    ////////////////////////////////////////////////////////////////////////////
    val deleStmt = testdbConn.prepareStatement("DROP TABLE IF EXISTS longdistancetravellers")
    deleStmt.executeUpdate()

    val createStmt = testdbConn.prepareStatement("""CREATE TABLE longdistancetravellers (
                                               |  id SERIAL PRIMARY KEY,
                                               |  bee_id VARCHAR(255) NOT NULL,
                                               |  square_count bigint
                                               |);""".stripMargin)
    createStmt.executeUpdate()
    val delestmt = testdbConn.prepareStatement("""DELETE FROM longdistancetravellers""")
    val qResult0 = delestmt.execute()


    val resetProducer = new KafkaProducer[String, Long](props)
    val resetConsumer = new KafkaConsumer[String, Long](props)

    resetConsumer.subscribe(Collections.singletonList("long-distance-travellers"))
    resetConsumer.poll(Duration.ofMillis(1000))
    val topicPartitions = resetConsumer.assignment().asScala

    resetConsumer.seekToEnd(topicPartitions.asJava)
    topicPartitions.foreach { tp =>
      val position = resetConsumer.position(tp)
      val record = new ProducerRecord[String, Long](
        "long-distance-travellers",
        tp.partition(),
        (position - 1).asInstanceOf[java.lang.Long],
        null.asInstanceOf[String],
        null.asInstanceOf[java.lang.Long]
      )
      resetProducer.send(record)
    }

    resetProducer.close()
    resetConsumer.close()
    ////////////////////////////////////////////////////////////////////////////

    val groupSize = 10
    val idGroup: List[UUID] = List.fill(groupSize)(UUID.randomUUID())

    val producer = new KafkaProducer[String, Long](props)

    def generateRandomEvent(count: Int): Seq[(String, Long)]= {
      (1 to count).map { _ =>
        val beeId = idGroup(new Random().nextInt(groupSize)).toString
        val square_count = (new Random().nextInt(5) + 3).toLong
        (beeId, square_count)
      }
    }
    ////////////////////////////////////////////////////////////////////////////
    val records = generateRandomEvent(5)

    records.foreach { singleRecord =>
      val record = new ProducerRecord[String, Long]("long-distance-travellers", singleRecord._1, singleRecord._2)
      producer.send(record)
    }
    producer.close()

    Thread.sleep(2000)

    var stmtList: List[(String, Long)] = List()

    val testConsumer = new KafkaConsumer[String, Long](props)

    val latch = new CountDownLatch(records.size)

    testConsumer.subscribe(Collections.singletonList("long-distance-travellers"))


    while (latch.getCount > 0) {
      val consumerRecords = testConsumer.poll(Duration.ofMillis(1000))
      SaveLongDistanceFlyers.processRecords(consumerRecords, testdbConn, latch)
    }
    latch.await()

    val stmt = testdbConn.prepareStatement("""SELECT bee_id, square_count FROM longdistancetravellers""")
    val qResults = stmt.executeQuery()


    while (qResults.next()) {
      stmtList = stmtList :+ (qResults.getString("bee_id"), qResults.getLong("square_count"))
    }

    assert(stmtList == records.toList)


  }





}
