package com.demo.common.serde;

import com.demo.common.model.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Generic JSON serialization / deserialization schemas for Flink Kafka connectors.
 *
 * Usage example:
 *   KafkaSource.<UserBehavior>builder()
 *       .setValueOnlyDeserializer(new JsonSchema.Deserializer<>(UserBehavior.class))
 *
 *   KafkaSink.<BehaviorWithDim>builder()
 *       .setRecordSerializer(KafkaRecordSerializationSchema.builder()
 *           .setValueSerializationSchema(new JsonSchema.Serializer<>(BehaviorWithDim.class))
 */
public final class JsonSchema {

    private JsonSchema() {}

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // ── Deserializer ─────────────────────────────────────────────────────────
    public static class Deserializer<T> implements DeserializationSchema<T> {

        private final Class<T> clazz;

        public Deserializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public T deserialize(byte[] message) throws IOException {
            if (message == null || message.length == 0) return null;
            return MAPPER.readValue(message, clazz);
        }

        @Override
        public boolean isEndOfStream(T nextElement) {
            return false;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return TypeInformation.of(clazz);
        }
    }

    // ── Serializer ───────────────────────────────────────────────────────────
    public static class Serializer<T> implements SerializationSchema<T> {

        private final Class<T> clazz;

        public Serializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte[] serialize(T element) {
            try {
                return MAPPER.writeValueAsBytes(element);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize " + clazz.getSimpleName(), e);
            }
        }
    }

    // ── Static helpers ───────────────────────────────────────────────────────
    public static <T> byte[] toBytes(T obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        try {
            return MAPPER.readValue(bytes, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
