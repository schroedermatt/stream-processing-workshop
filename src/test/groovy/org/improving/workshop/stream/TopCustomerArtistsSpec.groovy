package org.improving.workshop.stream

import net.datafaker.Faker
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.improving.workshop.domain.music.stream.Stream
import org.improving.workshop.domain.music.stream.StreamFaker
import spock.lang.Specification

class TopCustomerArtistsSpec extends Specification {
    TopologyTestDriver driver
    StreamFaker streamFaker
    TestInputTopic<String, Stream> inputTopic
    TestOutputTopic<String, LinkedHashMap<String, Long>> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the CustomerStreamCount topology (by reference)
        TopCustomerArtists.configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        inputTopic = driver.createInputTopic(
                TopCustomerArtists.INPUT_TOPIC,
                Serdes.String().serializer(),
                Streams.CUSTOMER_STREAM_JSON_SERDE.serializer()
        )

        outputTopic = driver.createOutputTopic(
                TopCustomerArtists.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Streams.LINKED_HASH_MAP_JSON_SERDE.deserializer()
        )

        streamFaker = new StreamFaker(new Faker())
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "customer top streamed artists"() {
        given: 'multiple customer streams received by the topology'
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "2"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "2"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "3"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "4"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "4"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "4"))

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == 6

        and: 'the last record holds the initial top 3 state'
        def top3 = outputRecords.last()

        // record is for customer 1
        top3.key() == "1"
        top3.value() == [
                "4": 3L,
                "2": 2L,
                "3": 1L
        ]

        when: 'streaming artist 5 twice'
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "5"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "5"))

        then: 'the latest top 3 has artist 5 present and artist 3 removed'
        def updatedTop3 = outputTopic.readRecordsToList().last()

        // record is for customer 1
        updatedTop3.key() == "1"
        // artist 3 dropped off the list and artist 5 joined the list
        updatedTop3.value() == [
                "4": 3L,
                "2": 2L,
                "5": 2L
        ]

        when: 'streaming artist 3 two more times'
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "3"))
        inputTopic.pipeInput(UUID.randomUUID().toString(), streamFaker.generate("1", "3"))

        then: 'the latest top 3 has artist 3 back in and artist 5 removed'
        def latestTop3 = outputTopic.readRecordsToList().last()

        // record is for customer 1
        latestTop3.key() == "1"
        // artist 3 is back into the top 3!
        latestTop3.value() == [
                "4": 3L,
                "3": 3L,
                "2": 2L
        ]
    }
}
