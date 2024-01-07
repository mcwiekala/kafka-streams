package io.confluent.developer.iot;

import static io.confluent.developer.StreamsUtils.getSpecificAvroSerde;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.iot.avro.Telemetry;
import io.confluent.iot.avro.TelemetryMessage;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

@Slf4j
public class IotStreams {


    public static void main(String[] args) throws IOException {

        final Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "telemetry-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("telemetry.input.topic");
        final String outputTopic = streamsProps.getProperty("telemetry.output.topic");
        final Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);
        SpecificAvroSerde<Telemetry> telemetrySerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<Telemetry> telemetryMessageSerde = getSpecificAvroSerde(configMap);

        builder.stream(inputTopic, Consumed.with(Serdes.String(), telemetrySerde))
            .peek((key, value) -> log.info("Incoming record - key: {} value: {} ", key, value))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(10)).grace(Duration.ofSeconds(30)))
            .aggregate(() -> new ArrayList<Telemetry>(),
                (key, telemetry, total) -> {
                    total.add(telemetry);
                    log.info("Key: {}, added: {}, elements in window: {}", key, telemetry, total.size());
                    return total;
                },
                Materialized.with(Serdes.String(), Serdes.ListSerde(ArrayList.class, telemetrySerde)))
            .suppress(Suppressed.untilWindowCloses(unbounded()))
            .toStream()
            .map((windowed, telemetryList) -> {
                Optional<Double> min = ((List<Telemetry>) telemetryList).stream()
                    .map(t -> t.getTemperature())
                    .min(Double::compareTo);
                Optional<Double> max = ((List<Telemetry>) telemetryList).stream()
                    .map(Telemetry::getTemperature)
                    .max(Double::compareTo);
                final Object key = ((Windowed) windowed).key();
                if (min.isEmpty() || max.isEmpty()) {
                    log.info("Empty");
                    return KeyValue.pair(key, null);
                }
                double temperatureDifference = max.get() - min.get();
                log.info("max {} min {}", max.get(), min.get());
                log.info("Key: {}, diff {}", key, temperatureDifference);
                return KeyValue.pair(key, new TelemetryMessage(key.toString(), (List<Telemetry>)telemetryList, temperatureDifference));
            })
            .filter((key, value) -> {
                if (value == null) {
                    return false;
                }
                TelemetryMessage telemetryMessage = (TelemetryMessage) value;
                if (telemetryMessage.getDiff() <= 10.) {
                    log.info(value + " - too low");
                    return false;
                }
                System.out.println("##################");
                log.info("ALERT! Shipment: {}", key);
                telemetryMessage.getTelemetries().forEach(telemetry ->
                    log.info("{} time - {}oC", Instant.ofEpochMilli(telemetry.getTimestamp()), telemetry.getTemperature()));
                System.out.println("##################");

                return true;
            })
            .peek((key, value) -> log.info("Output: " + key + " value " + value))
            .to(outputTopic, Produced.with(Serdes.String(), telemetryMessageSerde));

        runStreams(builder, streamsProps);
    }

    private static void runStreams(StreamsBuilder builder, Properties streamsProps) throws IOException {
        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
