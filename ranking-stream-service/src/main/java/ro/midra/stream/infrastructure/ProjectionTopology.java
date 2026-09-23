package ro.midra.stream.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.springframework.data.redis.core.StringRedisTemplate;

public final class ProjectionTopology {
    private ProjectionTopology() {
    }

    public static Topology build(String totals, StringRedisTemplate redis, MeterRegistry metrics) {
        var topology = new Topology();
        topology.addSource("totals-source", new StringDeserializer(), new StringDeserializer(), totals);
        topology.addProcessor(
                "project", () -> new ProjectionProcessor(redis, metrics), "totals-source");
        RankingTopology.addStore(topology, "projection", "project");
        RankingTopology.addStore(topology, "progress", "project");
        return topology;
    }
}
