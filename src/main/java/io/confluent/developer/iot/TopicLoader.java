package io.confluent.developer.iot;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.iot.avro.Telemetry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TopicLoader {

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    public static void runProducer() throws IOException {
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try(Admin adminClient = Admin.create(properties);
            Producer<String, Telemetry> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("telemetry.input.topic");
            final String outputTopic = properties.getProperty("telemetry.output.topic");
            var topics = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            adminClient.createTopics(topics);

            Callback callback = StreamsUtils.callback();

//            Instant instant = Instant.parse("2018-11-30T18:35:24.00Z");
            Instant instant = Instant.now();

            String shipmentId = UUID.randomUUID().toString();
            Telemetry telemetry = Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(21.2)
                .setTimestamp(instant.toEpochMilli())
                .build();

            instant = instant.plusSeconds(2);
            Telemetry telemetry2 = Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(25.2)
                .setTimestamp(instant.toEpochMilli())
                .build();

            instant = instant.plusSeconds(5);
            Telemetry telemetry3 = Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(32.2)
                .setTimestamp(instant.toEpochMilli())
                .build();

            instant = instant.plusSeconds(55);
            Telemetry telemetry4 = Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(33.2)
                .setTimestamp(instant.toEpochMilli())
                .build();

            instant = instant.plusSeconds(95);
            Telemetry telemetry5 = Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(37.2)
                .setTimestamp(instant.toEpochMilli())
                .build();

//            var telemetries = List.of(telemetry, telemetry2, telemetry3, telemetry4, telemetry5);
            var simulator = new ShipmentSimulator();
            var telemetries = simulator.simulateFail(50, Duration.ofSeconds(1));


            telemetries.forEach((t -> {
                ProducerRecord<String, Telemetry> producerRecord = new ProducerRecord<>(inputTopic,
                        0,
                        t.getTimestamp(),
                        t.getShipmentId(),
                        t);
                producer.send(producerRecord, callback);
            }));



        }
    }
}
