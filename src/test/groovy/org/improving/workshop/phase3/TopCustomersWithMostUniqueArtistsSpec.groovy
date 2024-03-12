package org.improving.workshop.phase3

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.stream.Stream
import spock.lang.Specification

import static org.improving.workshop.phase3.TopCustomersWithMostUniqueArtists.*
import static org.improving.workshop.utils.DataFaker.ARTISTS
import static org.improving.workshop.utils.DataFaker.CUSTOMERS
import static org.improving.workshop.utils.DataFaker.STREAMS

class TopCustomersWithMostUniqueArtistsSpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Stream> streamsTopic
    TestInputTopic<String, Customer> customerTopic
    TestInputTopic<String, Artist> artistTopic

    // outputs
    TestOutputTopic<String, CustomerUniqueArtistsList> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the CustomerStreamCount topology (by reference)
        configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        streamsTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_STREAMS, Serdes.String().serializer(), Streams.SERDE_STREAM_JSON.serializer())
        customerTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_CUSTOMERS, Serdes.String().serializer(), Streams.SERDE_CUSTOMER_JSON.serializer())
        artistTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_ARTISTS, Serdes.String().serializer(), Streams.SERDE_ARTIST_JSON.serializer())
        outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, Serdes.String().deserializer(), CUSTOMER_UNIQUE_ARTISTS_LIST_JSON_SERDE.deserializer())
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "customer top streamed artists"() {
        given: 'multiple customer streams for multiple artists received by the topology'
        def customers = new ArrayList<Customer>()
        for (int i = 0; i < 5; i++) {
            def customerId = "customer" + i + "-" + UUID.randomUUID().toString()
            def customer = CUSTOMERS.generate(customerId)
            customers.add(customer)
            customerTopic.pipeInput(customerId, customer)
        }

        def artists = new ArrayList<Artist>()
        for (int i = 0; i < 10; i++) {
            def artistId = "artist" + i + "-" + UUID.randomUUID().toString()
            def artist = ARTISTS.generate(artistId)
            artists.add(artist)
            artistTopic.pipeInput(artistId, artist)
        }

        def testCustomerUniqueArtistsMap = new HashMap<Customer, List<Artist>>()

        testCustomerUniqueArtistsMap.put(customers[0], [artists[0], artists[1], artists[2], artists[3], artists[4], artists[5], artists[6], artists[7], artists[8], artists[9]])
        testCustomerUniqueArtistsMap.put(customers[1], [artists[0], artists[2], artists[4], artists[6], artists[8]])
        testCustomerUniqueArtistsMap.put(customers[2], [artists[0], artists[1], artists[2], artists[3], artists[4]])
        testCustomerUniqueArtistsMap.put(customers[3], [artists[1], artists[3], artists[5], artists[7]])
        testCustomerUniqueArtistsMap.put(customers[4], [artists[9]])

        def random = new Random()
        def streamCount = 0
        testCustomerUniqueArtistsMap.each { customer, streamedArtistList ->
            // For each artist of a customer create a random number of streams for that customer.
            streamedArtistList.each { artist ->
                def numStreams = random.nextInt(1, 2)
                for (int i = 0; i < numStreams; i++) {
                    streamsTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customer.id(), artist.id()))
                    streamCount++
                }
            }
        }

        when: 'reading the output records'
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        outputRecords.size() == streamCount

        and:
        def top3 = outputRecords.last().value()
        top3.customerUniqueArtistsList.size() == 3
        top3.customerUniqueArtistsList.get(0).customer == customers[0]
        top3.customerUniqueArtistsList.get(0).uniqueCount == 10
        top3.customerUniqueArtistsList.get(0).uniqueArtistsSet.uniqueArtists.size() == testCustomerUniqueArtistsMap.get(customers[0]).size()
        top3.customerUniqueArtistsList.get(0).uniqueArtistsSet.uniqueArtists.containsAll(testCustomerUniqueArtistsMap.get(customers[0]))

        // TODO fix assertions below since 2nd and 3rd results have the same uniqueCount and are coming in out of order.
        top3.customerUniqueArtistsList.get(1).customer == customers[1]
        top3.customerUniqueArtistsList.get(1).uniqueCount == 5
        top3.customerUniqueArtistsList.get(1).uniqueArtistsSet.uniqueArtists.size() == testCustomerUniqueArtistsMap.get(customers[1]).size()
        top3.customerUniqueArtistsList.get(1).uniqueArtistsSet.uniqueArtists.containsAll(testCustomerUniqueArtistsMap.get(customers[1]))

        top3.customerUniqueArtistsList.get(2).customer == customers[2]
        top3.customerUniqueArtistsList.get(2).uniqueCount == 5
        top3.customerUniqueArtistsList.get(2).uniqueArtistsSet.uniqueArtists.size() == testCustomerUniqueArtistsMap.get(customers[1]).size()
        top3.customerUniqueArtistsList.get(2).uniqueArtistsSet.uniqueArtists.containsAll(testCustomerUniqueArtistsMap.get(customers[1]))


        println("End of Test")
    }
}
