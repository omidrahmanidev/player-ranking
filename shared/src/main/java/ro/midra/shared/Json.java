package ro.midra.shared;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Explicit JSON boundary for versioned Kafka contracts.
 */
public final class Json {
    private static final ObjectMapper MAPPER =
            JsonMapper.builder().addModule(new JavaTimeModule()).build();

    private Json() {
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize message", e);
        }
    }

    public static <T> T read(String value, Class<T> type) {
        try {
            return MAPPER.readValue(value, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid message", e);
        }
    }
}
