package org.improving.workshop.project

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.stream.Stream
import spock.lang.Specification

import static org.improving.workshop.utils.DataFaker.ARTISTS
import static org.improving.workshop.utils.DataFaker.STREAMS

class ConnecticutFavoriteArtistSpec extends Specification {
  TopologyTestDriver driver

  // inputs
  TestInputTopic<String, Stream> streamInputTopic
  TestInputTopic<String, Address> addressInputTopic
  TestInputTopic<String, Customer> customerInputTopic
  TestInputTopic<String, Artist> artistInputTopic

  // outputs
  TestOutputTopic<String, ConnecticutFavoriteArtist.ArtistStreams> outputTopic

  def 'setup'() {
    StreamsBuilder streamsBuilder = new StreamsBuilder()

    ConnecticutFavoriteArtist.configureTopology(streamsBuilder)

    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

    artistInputTopic = driver.createInputTopic(
      Streams.TOPIC_DATA_DEMO_ARTISTS,
      Serdes.String().serializer(),
      Streams.SERDE_ARTIST_JSON.serializer()
    )
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
      ConnecticutFavoriteArtist.CT_FAVORITE_ARTIST_TOPIC,
      Serdes.String().deserializer(),
      ConnecticutFavoriteArtist.ARTIST_STREAMS_JSON_SERDE.deserializer()
    )
  }

  def 'cleanup'() {
    driver.close()
  }

  // Test to ensure simply that events are correctly flowing
  def "it returns the expected artist when there is only 1 artist and only 1 customer in CT streaming"() {
    given: 'an artist'
    String favoriteArtistId = "ct-favorite"
    artistInputTopic.pipeInput(favoriteArtistId, ARTISTS.generate(favoriteArtistId))

    and: 'A customer with the CT address'
    String customerId = 'ct-customer'
    String addressId = 'ct-address'
    addressInputTopic.pipeInput(addressId, new Address(
      addressId,
      customerId,
      "US",
      "Residential",
      "123 4th st",
      "",
      "Hartford",
      "CT",
      "06101",
      "1234",
      "US"
    ))
    customerInputTopic.pipeInput(customerId, new Customer(
      customerId,
      "test-customer",
      "m",
      "John",
      "",
      "Smith",
      "John Smith",
      "Jr",
      "",
      "1-2-99",
      "12-12-12"
    ))

    and: 'the CT customer streaming the artist'
    streamInputTopic.pipeInput(STREAMS.generate(customerId, favoriteArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(customerId, favoriteArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(customerId, favoriteArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(customerId, favoriteArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(customerId, favoriteArtistId))
//
//
    when: 'reading the output records'
    def outputRecords = outputTopic.readRecordsToList()

    then: 'the artist is given as the favorite artist'
    outputRecords.size() == 5
    def favoriteArtist = outputRecords.last().value()
    favoriteArtist.artistid == favoriteArtistId
    favoriteArtist.totalStreams == 5L
  }

  // Test to ensure that streams from out of CT are not considered
  def "it returns the expected artist when there are streams from customers outside CT"() {
    given: '2 artists'
    String favoriteArtistId = 'ct-artist'
    String otherArtistId = 'other-artist'
    artistInputTopic.pipeInput(favoriteArtistId, ARTISTS.generate(favoriteArtistId))
    artistInputTopic.pipeInput(otherArtistId, ARTISTS.generate(otherArtistId))

    and: 'a CT customer'
    String ctCustomerId = 'ct-customer'
    String ctAddressId = 'ct-address'
    addressInputTopic.pipeInput(ctAddressId, new Address(
      ctAddressId,
      ctCustomerId,
      "US",
      "Residential",
      "123 4th st",
      "",
      "Hartford",
      "CT",
      "06101",
      "1234",
      "US"
    ))
    customerInputTopic.pipeInput(ctCustomerId, new Customer(
      ctCustomerId,
      "test-customer",
      "m",
      "John",
      "",
      "Smith",
      "John Smith",
      "Jr",
      "",
      "01-02-1999",
      "12-12-2012"
    ))

    and: 'a non-CT customer'
    String nonCtCustomerId = 'non-ct-customer'
    String nonCtAddressId = 'non-ct-address'
    addressInputTopic.pipeInput(nonCtAddressId, new Address(
      nonCtAddressId,
      nonCtCustomerId,
      "US",
      "Residential",
      "987 6th ave",
      "",
      "Los Angeles",
      "CA",
      "90210",
      "4321",
      "US"
    ))
    customerInputTopic.pipeInput(nonCtCustomerId, new Customer(
      nonCtCustomerId,
      "test-customer",
      "f",
      "Jane",
      "",
      "Doe",
      "Jane Doe",
      "",
      "Ms",
      "12-11-1990",
      "01-02-2013"
    ))

    and: 'a stream from the ct customer only to the favorite artist'
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, favoriteArtistId))

    and: 'multiple streams from the non-ct customer only to the other artist'
    streamInputTopic.pipeInput(STREAMS.generate(nonCtCustomerId, otherArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(nonCtCustomerId, otherArtistId))

    when: 'reading output records'
    def outputRecords = outputTopic.readRecordsToList()

    then: 'the only output record is the favorite artist with 1 stream'
    outputRecords.size() == 1
    def favoriteArtist = outputRecords.first().value()
    favoriteArtist.artistid == favoriteArtistId
    favoriteArtist.totalStreams == 1L
  }

  // Test to ensure that the top artist is correctly identified
  def "it returns the expected artist when there are streams for multiple artists from customers only in CT"() {
    given: '2 artists'
    String favoriteArtistId = 'ct-artist'
    String otherArtistId = 'other-artist'
    artistInputTopic.pipeInput(favoriteArtistId, ARTISTS.generate(favoriteArtistId))
    artistInputTopic.pipeInput(otherArtistId, ARTISTS.generate(otherArtistId))

    and: '1 CT customer'
    String ctCustomerId = 'ct-customer'
    String ctAddressId = 'ct-address'
    addressInputTopic.pipeInput(ctAddressId, new Address(
      ctAddressId,
      ctCustomerId,
      "US",
      "Residential",
      "123 4th st",
      "",
      "Hartford",
      "CT",
      "06101",
      "1234",
      "US"
    ))
    customerInputTopic.pipeInput(ctCustomerId, new Customer(
      ctCustomerId,
      "test-customer",
      "m",
      "John",
      "",
      "Smith",
      "John Smith",
      "Jr",
      "",
      "01-02-1999",
      "12-12-2012"
    ))


    and: '2 streams to the favorite artist'
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, favoriteArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, favoriteArtistId))

    and: 'then 1 stream to the other artist'
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, otherArtistId))

    when: 'reading output records'
    def outputRecords = outputTopic.readRecordsToList()

    then: 'the only output record is the favorite artist with 2 streams'
    // There are still 2 records because a new one publishes to update the stream count even it it's the current
    // favorite artist
    outputRecords.size() == 2

    def firstFavorite = outputRecords.first().value()
    firstFavorite.artistid == favoriteArtistId
    firstFavorite.totalStreams == 1L

    def secondFavorite = outputRecords.last().value()
    secondFavorite.artistid == favoriteArtistId
    secondFavorite.totalStreams == 2L
  }

  // Test to ensure that the top artist is correctly identified
  def "it returns the expected artists when there are streams for multiple artists from customers only in CT and one artist overtakes the other"() {
    given: '2 artists'
    String finalFavoriteArtistId = 'ct-final-favorite-artist'
    String initialFavoriteArtistId = 'ct-initial-favorite-artist'
    artistInputTopic.pipeInput(finalFavoriteArtistId, ARTISTS.generate(finalFavoriteArtistId))
    artistInputTopic.pipeInput(initialFavoriteArtistId, ARTISTS.generate(initialFavoriteArtistId))

    and: '1 CT customer'
    String ctCustomerId = 'ct-customer'
    String ctAddressId = 'ct-address'
    addressInputTopic.pipeInput(ctAddressId, new Address(
      ctAddressId,
      ctCustomerId,
      "US",
      "Residential",
      "123 4th st",
      "",
      "Hartford",
      "CT",
      "06101",
      "1234",
      "US"
    ))
    customerInputTopic.pipeInput(ctCustomerId, new Customer(
      ctCustomerId,
      "test-customer",
      "m",
      "John",
      "",
      "Smith",
      "John Smith",
      "Jr",
      "",
      "01-02-1999",
      "12-12-2012"
    ))

    and: 'then 1 stream to the other artist'
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, initialFavoriteArtistId))

    and: '2 streams to the favorite artist'
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, finalFavoriteArtistId))
    streamInputTopic.pipeInput(STREAMS.generate(ctCustomerId, finalFavoriteArtistId))

    when: 'reading output records'
    def outputRecords = outputTopic.readRecordsToList()

    then: 'there are 2 favorites output'
    outputRecords.size() == 2

    and: 'the first was the initial favorite artist'
    def firstFavoriteArtist = outputRecords.first().value()
    firstFavoriteArtist.artistid == initialFavoriteArtistId
    firstFavoriteArtist.totalStreams == 1L

    and: 'the second was the final favorite artist'
    def secondFavoriteArtist = outputRecords.last().value()
    secondFavoriteArtist.artistid == finalFavoriteArtistId
    secondFavoriteArtist.totalStreams == 2L
  }
}