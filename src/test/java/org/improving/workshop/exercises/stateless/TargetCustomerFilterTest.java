package org.improving.workshop.exercises.stateless;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.customer.profile.Customer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TargetCustomerFilterTest {

  private final static Serializer<String> stringSerializer = Serdes.String().serializer();
  private final static Deserializer<String> stringDeserializer = Serdes.String().deserializer();

  private TopologyTestDriver driver;
  private TestInputTopic<String, Customer> customerInputTopic;
  private TestInputTopic<String, Customer> legacyCustomerInputTopic;
  private TestOutputTopic<String, String> outputTopic;

  @BeforeEach
  void setup() {
    // instantiate new builder
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // build the TargetCustomerFilter topology (by reference)
    TargetCustomerFilter.configureTopology(streamsBuilder);

    // build the TopologyTestDriver
    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    customerInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_CUSTOMERS,
            stringSerializer,
            Streams.SERDE_CUSTOMER_JSON.serializer()
    );

    legacyCustomerInputTopic = driver.createInputTopic(
            TargetCustomerFilter.LEGACY_INPUT_TOPIC,
            stringSerializer,
            Streams.SERDE_CUSTOMER_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            TargetCustomerFilter.OUTPUT_TOPIC,
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
  void targetCustomerFilterCaptures1990sCustomers() {
    var cust1 = new Customer("123", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02");
    var cust2 = new Customer("456", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02");
    var cust3 = new Customer("567", "PREMIUM", "M", "George", "Mo", "James", "GMJ", "", "", "1999-01-20", "2022-01-02");
    var cust4 = new Customer("678", "PREMIUM", "M", "Tommy", "Toe", "James", "TTJ", "", "", "2000-01-20", "2022-01-02");

    customerInputTopic.pipeKeyValueList(List.of(
            new KeyValue<String, Customer>(cust1.id(), cust1),
            new KeyValue<String, Customer>(cust2.id(), cust2),
            new KeyValue<String, Customer>(cust3.id(), cust3),
            new KeyValue<String, Customer>(cust4.id(), cust4)
    ));

    //reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    //two records came through - 1990 & 1999 birthdt customers
    assertEquals(2, outputRecords.size());

    //and they were customer 2 and 3
    assertEquals(outputRecords.getFirst().key(), cust2.id());
    assertEquals(outputRecords.getLast().key(), cust3.id());
  }

  @Test
  void BONUSLegacyCustomerTopicIsAlsoCaptured() {

    var cust1 = new Customer("123", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02");
    var cust2 = new Customer("456", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02");
    var cust3 = new Customer("567", "PREMIUM", "M", "George", "Mo", "James", "GMJ", "", "", "1999-01-20", "2022-01-02");
    var cust4 = new Customer("678", "PREMIUM", "M", "Tommy", "Toe", "James", "TTJ", "", "", "2000-01-20", "2022-01-02");

    // when piping the customers through the NEW topic
    customerInputTopic.pipeKeyValueList(List.of(
            new KeyValue<String, Customer>(cust1.id(), cust1),
            new KeyValue<String, Customer>(cust2.id(), cust2)
    ));

    //and piping customers through the LEGACY topic
    legacyCustomerInputTopic.pipeKeyValueList(List.of(
            new KeyValue<String, Customer>(cust3.id(), cust3),
            new KeyValue<String, Customer>(cust4.id(), cust4)
    ));

    //then reading the output records
    var outputRecords = outputTopic.readRecordsToList();

    // two records came through - 1990 & 1999 birthdt customers
    assertEquals(2, outputRecords.size());

    // and they were customer 2 and 3
    assertEquals(outputRecords.getFirst().key(), cust2.id());
    assertEquals(outputRecords.getLast().key(), cust3.id());
  }

}
