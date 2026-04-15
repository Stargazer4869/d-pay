package com.dpay.common.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .findAndRegisterModules();

    private JsonUtils() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize value to json", exception);
        }
    }

    public static <T> T fromJson(String value, Class<T> type) {
        try {
            return MAPPER.readValue(value, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to deserialize json", exception);
        }
    }

    public static <T> T fromJson(String value, TypeReference<T> type) {
        try {
            return MAPPER.readValue(value, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to deserialize json", exception);
        }
    }
}
