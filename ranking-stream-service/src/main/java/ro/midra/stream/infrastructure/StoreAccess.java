package ro.midra.stream.infrastructure;

import org.apache.kafka.streams.state.KeyValueStore;
import ro.midra.shared.Json;

final class StoreAccess {
    private StoreAccess() {
    }

    static <T> T read(KeyValueStore<String, String> store, String key, Class<T> type, T fallback) {
        String value = store.get(key);
        return value == null ? fallback : Json.read(value, type);
    }

    static String timedKey(long time, String identity) {
        return "%019d:%s".formatted(time, identity);
    }
}
