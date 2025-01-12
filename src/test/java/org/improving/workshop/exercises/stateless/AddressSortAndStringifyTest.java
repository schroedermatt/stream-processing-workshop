package org.improving.workshop.exercises.stateless;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.address.Address;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AddressSortAndStringifyTest {

  private final static Serializer<String> stringSerializer = Serdes.String().serializer();
  private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

  private TopologyTestDriver driver;
  private TestInputTopic<String, Address> addressInputTopic;
  private TestOutputTopic<String, String> outputTopic;
  private TestOutputTopic<String, String> mnOutputTopic;

  @BeforeEach
  void setup() {
    // instantiate new builder
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // build the AddressSortAndStringify topology (by reference)
    AddressSortAndStringify.configureTopology(streamsBuilder);

    // build the TopologyTestDriver
    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    addressInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_ADDRESSES,
            stringSerializer,
            Streams.SERDE_ADDRESS_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            AddressSortAndStringify.DEFAULT_OUTPUT_TOPIC,
            stringDeserializer,
            stringDeserializer
    );

    mnOutputTopic = driver.createOutputTopic(
            AddressSortAndStringify.MN_OUTPUT_TOPIC,
            stringDeserializer,
            stringDeserializer
    );
  }

  @AfterEach
  void cleanup() {
    // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
    // run the test and let it cleanup, then run the test again.
    driver.close();
  }

  @Test
  void wiAddressRekeyAndStringify() {

    var addressId = "address-123";
    var address = new Address(
            addressId, "cust-678", "cd", "HOME", "111 1st St", "Apt 2",
            "Madison", "WI", "55555", "1234", "USA", 0.0, 0.0);

    addressInputTopic.pipeInput(addressId, address);

    var outputRecords = outputTopic.readRecordsToList();

    assertEquals(1, outputRecords.size());
    assertEquals(address.state(), outputRecords.getFirst().key());
    assertEquals("111 1st St, Apt 2, Madison, WI 55555-1234 USA", outputRecords.getFirst().value());
    assertTrue(mnOutputTopic.isEmpty());
  }

  @Test
  void bonusMNAddressSplit() {

    var addressId = "address-123";
    var address = new Address(
            addressId, "cust-123", "cd", "BUS", "222 1st St", "Suite 4",
            "Minneapolis", "MN", "55419", "1234", "USA", 0.0, 0.0);

    addressInputTopic.pipeInput(addressId, address);

    //zero records came through the default path
    assertTrue(outputTopic.isEmpty());

    //one record came through the MN branch
    var outputRecords = mnOutputTopic.readRecordsToList();
    assertEquals(1, outputRecords.size());

    //the key was changed to be the addresses state
    assertEquals(address.state(), outputRecords.getFirst().key());

    //the value was stringified
    assertEquals("222 1st St, Suite 4, Minneapolis, MN 55419-1234 USA", outputRecords.getFirst().value());
  }

}
