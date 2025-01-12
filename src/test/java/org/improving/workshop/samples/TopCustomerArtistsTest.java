package org.improving.workshop.samples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.stream.Stream;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TopCustomerArtistsTest {

  private TopologyTestDriver driver;

  // inputs
  private TestInputTopic<String, Stream> inputTopic;

  // outputs
  private TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic;

  @BeforeEach
  void setup() {
    // instantiate new builder
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // build the CustomerStreamCount topology (by reference)
    TopCustomerArtists.configureTopology(streamsBuilder);

    // build the TopologyTestDriver
    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    inputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            Serdes.String().serializer(),
            Streams.SERDE_STREAM_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            TopCustomerArtists.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            TopCustomerArtists.LINKED_HASH_MAP_JSON_SERDE.deserializer()
    );
  }

  @AfterEach
  void cleanup() {
    // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
    // run the test and let it cleanup, then run the test again.
    driver.close();
  }

  @Test
  @DisplayName("customer top streamed artists")
  void customerTopStreamedArtists() {
    //multiple customer streams received by the topology
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "2"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "2"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "3"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "4"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "4"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "4"));

    //reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    //the expected number of records were received
    assertEquals(6, outputRecords.size());

    //the last record holds the initial top 3 state
    assertEquals(
            java.util.stream.Stream.of(
                    Map.entry("4", 3L),
                    Map.entry("2", 2L),
                    Map.entry("3", 1L)
            ).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll),
            outputRecords.getLast().value());

    //streaming artist 5 twice
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "5"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "5"));

    //the latest top 3 has artist 5 present and artist 3 removed
    assertEquals(
            java.util.stream.Stream.of(
                    Map.entry("4", 3L),
                    Map.entry("2", 2L),
                    Map.entry("5", 2L)
            ).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll),
            outputTopic.readRecordsToList().getLast().value());

    //streaming artist 3 two more times
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "3"));
    inputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("1", "3"));

    //the latest top 3 has artist 3 back in and artist 5 removed
    assertEquals(
            java.util.stream.Stream.of(
                    Map.entry("4", 3L),
                    Map.entry("3", 3L),
                    Map.entry("2", 2L)
            ).collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll),
            outputTopic.readRecordsToList().getLast().value());
  }

}
