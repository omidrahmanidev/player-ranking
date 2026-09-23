package ro.midra.stream.infrastructure;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import ro.midra.shared.Json;
import ro.midra.stream.domain.WindowPolicy;

import java.time.Clock;

/**
 * Preserves source partitions for totals and clock markers, avoiding a global aggregation
 * bottleneck.
 */
public final class RankingTopology {
    private RankingTopology() {
    }

    public static Topology build(
            Clock clock, WindowPolicy policy, String scores, String totals, String audit) {
        var topology = new Topology();
        topology.addSource("scores-source", new StringDeserializer(), new StringDeserializer(), scores);
        topology.addProcessor("window", () -> new WindowProcessor(clock, policy), "scores-source");
        for (String name : WindowProcessor.STORES) addStore(topology, name, "window");
        topology.addSink(
                "totals-sink",
                totals,
                new StringSerializer(),
                new StringSerializer(),
                (topic, key, value, partitions) ->
                        Json.read(value, RankingUpdate.class).partition(),
                "window");
        return addAuditAndPartitioning(topology, audit);
    }

    private static Topology addAuditAndPartitioning(Topology topology, String audit) {
        topology.addSink("audit-sink", audit, new StringSerializer(), new StringSerializer(), "window");
        return topology;
    }

    static void addStore(Topology topology, String name, String processor) {
        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(name), Serdes.String(), Serdes.String()),
                processor);
    }
}
