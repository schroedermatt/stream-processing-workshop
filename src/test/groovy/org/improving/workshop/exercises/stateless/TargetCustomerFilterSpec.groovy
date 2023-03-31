package org.improving.workshop.exercises.stateless

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.profile.Customer
import spock.lang.Specification

class TargetCustomerFilterSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Customer> customerInputTopic

    // outputs - customerid, customer
    TestOutputTopic<String, String> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the TargetCustomerFilter topology (by reference)
        TargetCustomerFilter.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                TargetCustomerFilter.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "target customer filter captures 1990s customers"() {
        given: 'a set of customers'
        def cust1 = new Customer("123", "PREMIUM", "M", "John", "Steven", "James", "JSJ", "", "", "1989-01-20", "2022-01-02")
        def cust2 = new Customer("456", "PREMIUM", "M", "Jane", "Jo", "James", "JJJ", "", "", "1990-01-20", "2022-01-02")
        def cust3 = new Customer("567", "PREMIUM", "M", "George", "Mo", "James", "GMJ", "", "", "1999-01-20", "2022-01-02")
        def cust4 = new Customer("678", "PREMIUM", "M", "Tommy", "Toe", "James", "TTJ", "", "", "2000-01-20", "2022-01-02")

        when: 'piping the customers through the stream'
        customerInputTopic.pipeKeyValueList([
                new KeyValue<String, Customer>(cust1.id(), cust1),
                new KeyValue<String, Customer>(cust2.id(), cust2),
                new KeyValue<String, Customer>(cust3.id(), cust3),
                new KeyValue<String, Customer>(cust4.id(), cust4),
        ])

        then: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'two records came through - 1990 & 1999 birthdt customers'
        outputRecords.size() == 2

        and: 'they were customer 2 and 3'
        outputRecords.first().key() == cust2.id()
        outputRecords.last().key() == cust3.id()
    }
}
