package org.improving.workshop.exercises.stateful;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.stream.Stream;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CustomerStreamCounterTest {

  private final static Serializer<String> stringSerializer = Serdes.String().serializer();
  private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();
  private final static Deserializer<Long> longDeserializer = Serdes.Long().deserializer();

  private TopologyTestDriver driver;

  // inputs
  private TestInputTopic<String, Stream> inputTopic;

  // outputs
  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  public void setup() {
    // instantiate new builder
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // build the CustomerStreamCount topology (by reference)
    CustomerStreamCount.configureTopology(streamsBuilder);

    // build the TopologyTestDriver
    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    inputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            stringSerializer,
            Streams.SERDE_STREAM_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            CustomerStreamCount.OUTPUT_TOPIC,
            stringDeserializer,
            longDeserializer
    );

  }

  @AfterEach
  public void cleanup() {
    driver.close();
  }


  @Test
  void customerStreamCounter() {

    inputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate("1", "2"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate("1", "3"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate("1", "4"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), DataFaker.STREAMS.generate("2", "3"));

    List<TestRecord<String, Long>> outputRecords = outputTopic.readRecordsToList();

    assertEquals(4, outputRecords.size());

    // customer 1, stream 1
    assertEquals("1", outputRecords.get(0).key());
    assertEquals(1L, outputRecords.get(0).value());

    // customer 1, stream 2
    assertEquals("1", outputRecords.get(1).key());
    assertEquals(2L, outputRecords.get(1).value());

    // customer 1, stream 3
    assertEquals("1", outputRecords.get(2).key());
    assertEquals(3L, outputRecords.get(2).value());

    // customer 2, stream 1
    assertEquals("2", outputRecords.get(3).key());
    assertEquals(1L, outputRecords.get(3).value());
  }

}