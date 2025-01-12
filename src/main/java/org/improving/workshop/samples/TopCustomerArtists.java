package org.improving.workshop.samples;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class TopCustomerArtists {


  // MUST BE PREFIXED WITH "kafka-workshop-"
  public static final String OUTPUT_TOPIC = "kafka-workshop-top-10-stream-count";

  public static final JsonSerde<SortedCounterMap> COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);

  // Jackson is converting Value into Integer Not Long due to erasure,
  //public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE = new JsonSerde<>(LinkedHashMap.class);
  public static final JsonSerde<LinkedHashMap<String, Long>> LINKED_HASH_MAP_JSON_SERDE
          = new JsonSerde<>(
          new TypeReference<LinkedHashMap<String, Long>>() {
          },
          new ObjectMapper()
                  .configure(DeserializationFeature.USE_LONG_FOR_INTS, true)
  );

  /**
   * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
   */
  public static void main(final String[] args) {
    final StreamsBuilder builder = new StreamsBuilder();

    // configure the processing topology
    configureTopology(builder);

    // fire up the engines
    startStreams(builder);
  }

  static void configureTopology(final StreamsBuilder builder) {
    builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .peek((streamId, stream) -> log.info("Stream Received: {}", stream))

            // rekey so that the groupBy is by customerid and not streamid
            // groupBy is shorthand for (selectKey + groupByKey)
            .groupBy((k, v) -> v.customerid())

            // keep track of each customer's artist stream counts in a ktable
            .aggregate(
                    // initializer
                    SortedCounterMap::new,

                    // aggregator
                    (customerId, stream, customerArtistStreamCounts) -> {
                      customerArtistStreamCounts.incrementCount(stream.artistid());
                      return customerArtistStreamCounts;
                    },

                    // ktable (materialized) configuration
                    Materialized
                            .<String, SortedCounterMap>as(persistentKeyValueStore("customer-artist-stream-counts"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(COUNTER_MAP_JSON_SERDE)
            )

            // turn it back into a stream so that it can be produced to the OUTPUT_TOPIC
            .toStream()
            // trim to only the top 3
            .mapValues(sortedCounterMap -> sortedCounterMap.top(3))
            .peek((key, counterMap) -> log.info("Customer {}'s Top 3 Streamed Artists: {}", key, counterMap))
            // NOTE: when using ccloud, the topic must exist or 'auto.create.topics.enable' set to true (dedicated cluster required)
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), LINKED_HASH_MAP_JSON_SERDE));
  }

  @Data
  @AllArgsConstructor
  public static class SortedCounterMap {
    private int maxSize;
    private LinkedHashMap<String, Long> map;

    public SortedCounterMap() {
      this(1000);
    }

    public SortedCounterMap(int maxSize) {
      this.maxSize = maxSize;
      this.map = new LinkedHashMap<>();
    }

    public void incrementCount(String id) {
      map.compute(id, (k, v) -> v == null ? 1 : v + 1);

      // replace with sorted map
      this.map = map.entrySet().stream()
              .sorted(reverseOrder(Map.Entry.comparingByValue()))
              // keep a limit on the map size
              .limit(maxSize)
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    /**
     * Return the top {limit} items from the counter map
     *
     * @param limit the number of records to include in the returned map
     * @return a new LinkedHashMap with only the top {limit} elements
     */
    public LinkedHashMap<String, Long> top(int limit) {
      return map.entrySet().stream()
              .limit(limit)
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
  }
}