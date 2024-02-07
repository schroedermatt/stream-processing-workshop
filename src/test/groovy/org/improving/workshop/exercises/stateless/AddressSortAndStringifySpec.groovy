package org.improving.workshop.exercises.stateless

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.address.Address
import spock.lang.Specification

class AddressSortAndStringifySpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Address> addressInputTopic

    // outputs - addressid, address (string)
    TestOutputTopic<String, String> outputTopic
    TestOutputTopic<String, String> mnOutputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the AddressSortAndStringify topology (by reference)
        AddressSortAndStringify.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                AddressSortAndStringify.DEFAULT_OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )

        mnOutputTopic = driver.createOutputTopic(
                AddressSortAndStringify.MN_OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "WI address rekey and stringify"() {
        given: 'an address'
        def addressId = "address-123"
        def address = new Address(
                addressId, "cust-678", "cd", "HOME", "111 1st St", "Apt 2",
                "Madison", "WI", "55555", "1234", "USA", 0L, 0L)

        when: 'piping the address through the stream'
        addressInputTopic.pipeInput(addressId, address)

        then: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'one record came through'
        outputRecords.size() == 1

        and: 'the key was changed to be the addresses state'
        outputRecords.first().key() == address.state()

        and: 'the value was stringified'
        outputRecords.first().value() == "111 1st St, Apt 2, Madison, WI 55555-1234 USA"

        and: 'zero mn records came through'
        mnOutputTopic.isEmpty()
    }

    def "bonus - MN address split"() {
        given: 'an address'
        def addressId = "address-123"
        def address = new Address(
                addressId, "cust-123", "cd", "BUS", "222 1st St", "Suite 4",
                "Minneapolis", "MN", "55419", "1234", "USA", 0L, 0L)

        when: 'piping the address through the stream'
        addressInputTopic.pipeInput(addressId, address)

        then: 'zero records came through the default path'
        outputTopic.isEmpty()

        and: 'one record came through the MN branch'
        def outputRecords = mnOutputTopic.readRecordsToList()
        outputRecords.size() == 1

        and: 'the key was changed to be the addresses state'
        outputRecords.first().key() == address.state()

        and: 'the value was stringified'
        outputRecords.first().value() == "222 1st St, Suite 4, Minneapolis, MN 55419-1234 USA"
    }
}
