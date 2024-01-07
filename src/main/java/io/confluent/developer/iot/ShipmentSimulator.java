package io.confluent.developer.iot;

import io.confluent.iot.avro.Telemetry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

class ShipmentSimulator {

    List<Telemetry> simulateFail(Integer recordsNumber, Duration timeInterval) {
        List<Telemetry> result = new ArrayList<>();
        Instant instant = Instant.now();
        double temperature = 21.2;

        String shipmentId = UUID.randomUUID().toString();
        for (int i = 0; i < recordsNumber; i++) {
            instant = instant.plus(timeInterval);
            temperature += i;
            if(temperature > 500) {
                temperature = 500;
            }

            result.add(Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(temperature)
                .setTimestamp(instant.toEpochMilli())
                .build());
        }

        return result;
    }

    List<Telemetry> simulateNormal(Integer recordsNumber, Duration timeInterval, double temperatureStart) {
        List<Telemetry> result = new ArrayList<>();
        Instant instant = Instant.now();
        Random random = new Random();

        String shipmentId = UUID.randomUUID().toString();
        for (int i = 0; i < recordsNumber; i++) {
            instant = instant.plus(timeInterval);
            temperatureStart += random.nextInt(5) - 2;

            result.add(Telemetry.newBuilder()
                .setTelemetryId(UUID.randomUUID().toString())
                .setShipmentId(shipmentId)
                .setTemperature(temperatureStart)
                .setTimestamp(instant.toEpochMilli())
                .build());
        }

        return result;
    }

    public static void main(String[] args) {
        List<Telemetry> simulate = new ShipmentSimulator().simulateNormal(100, Duration.ofSeconds(5), 21.2);
        System.out.println(simulate);
    }

}
