package org.improving.workshop.samples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.stream.Stream
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.STREAMS

class ArtistStreamByStateMetricsSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Customer> customerInputTopic
    TestInputTopic<String, Address> addressInputTopic
    TestInputTopic<String, Stream> streamInputTopic

    // outputs
    TestOutputTopic<String, ArtistStreamByStateMetrics.ArtistStateMetrics> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the ArtistMetrics topology (by reference)
        ArtistStreamByStateMetrics.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        addressInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ADDRESSES,
                Serdes.String().serializer(),
                Streams.SERDE_ADDRESS_JSON.serializer()
        )

        customerInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_CUSTOMERS,
                Serdes.String().serializer(),
                Streams.SERDE_CUSTOMER_JSON.serializer()
        )

        streamInputTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_STREAMS,
                Serdes.String().serializer(),
                Streams.SERDE_STREAM_JSON.serializer()
        )

        outputTopic = driver.createOutputTopic(
                ArtistStreamByStateMetrics.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                ArtistStreamByStateMetrics.ARTIST_STATE_METRICS_JSON_SERDE.deserializer()
        )
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "state level streaming counts"() {
        given: 'two customers'
        def cust = generateCustomer("customer-1")
        customerInputTopic.pipeInput(cust.id(), cust)
        def cust2 = generateCustomer("customer-2")
        customerInputTopic.pipeInput(cust2.id(), cust2)

        and: 'two addresses'
        def add = generateAddress("add1", cust.id(), "MN")
        addressInputTopic.pipeInput(add.id(), add)
        def add2 = generateAddress("add2", cust2.id(), "WI")
        addressInputTopic.pipeInput(add2.id(), add2)

        and: 'streaming 12 total times'
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", "a1"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", "a1"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a1"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a1"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a1"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", "a2"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-1", "a2"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a2"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a2"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a2"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a2"))
        streamInputTopic.pipeInput(STREAMS.generate("customer-2", "a2"))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 12

        and: 'MN metrics are accurate'
        with (outputRecords.get(6)) {
            assert it.key() == 'MN'

            assert it.value().metrics.get("a1") == 2
            assert it.value().metrics.get("a2") == 2
        }

        and: 'WI metrics are accurate'
        with (outputRecords.last()) {
            assert it.key() == 'WI'

            assert it.value().metrics.get("a1") == 3
            assert it.value().metrics.get("a2") == 5
        }
    }

    Address generateAddress(String id, String customerId, String state) {
        return new Address(id, customerId, null, null, null, null, null, state, null, null, null)
    }

    Customer generateCustomer(String id) {
        return new Customer(id, null, null, null, null, null, null, null, null, null, null)
    }
}
