package ro.midra.integration;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ro.midra.query.domain.RankingResult;
import ro.midra.shared.Json;
import ro.midra.shared.ScoreEvent;
import ro.midra.stream.domain.WindowPolicy;
import ro.midra.stream.infrastructure.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SystemIT {
    private static final int PARTITIONS = 2;

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.9.1"));

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.6-alpine")).withExposedPorts(6379);

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @TempDir
    static Path directory;
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final AdjustableClock CLOCK = new AdjustableClock();
    private static LettuceConnectionFactory connection;
    private static StringRedisTemplate redis;
    private static KafkaStreams calculator, projector;
    private static ConfigurableApplicationContext score, query, gateway, stream;
    private static String baseUrl;
    private static JdbcTemplate jdbc;
    private static ScoreEvent retryEvent;

    @BeforeAll
    static void start() throws Exception {
        createTopics();
        connection = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connection.afterPropertiesSet();
        redis = new StringRedisTemplate(connection);
        var dataSource =
                new DriverManagerDataSource(
                        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        startStreams("first");
        score = startWeb(ro.midra.score.Application.class, "score-service");
        query = startWeb(ro.midra.query.Application.class, "ranking-query-service");
        gateway =
                startWeb(
                        ro.midra.gateway.Application.class,
                        "api-gateway",
                        "--SCORE_SERVICE_URL=http://localhost:" + port(score),
                        "--RANKING_QUERY_SERVICE_URL=http://localhost:" + port(query));
        baseUrl = "http://localhost:" + port(gateway);
        await()
                .atMost(Duration.ofSeconds(90))
                .ignoreExceptions()
                .until(() -> get("/top").statusCode() == 200);
    }

    @AfterAll
    static void close() {
        for (var context : Arrays.asList(gateway, query, score, stream))
            if (context != null) context.close();
        closeStreams();
        if (connection != null) connection.destroy();
    }

    @Test
    @Order(1)
    void restGatewayTopRankAndNeighbors() throws Exception {
        for (int i = 1; i <= 15; i++) post(event("p%02d".formatted(i), i * 10, Instant.now()));
        await()
                .atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            RankingResult top = result("/top");
                            assertThat(top.totalPlayers()).isEqualTo(15);
                            assertThat(top.players()).hasSize(10);
                            assertThat(top.players().getFirst().playerId()).isEqualTo("p15");
                            assertThat(top.players().getFirst().rank()).isEqualTo(1);
                        });
        assertThat(result("/players/p08/rank").players().getFirst().rank()).isEqualTo(8);
        assertThat(result("/players/p08/neighbors").players())
                .extracting("playerId")
                .containsExactly("p10", "p09", "p08", "p07", "p06");
        assertThat(result("/players/p15/neighbors").players()).hasSize(3);
        assertThat(result("/players/p01/neighbors").players()).hasSize(3);
        assertThat(get("/players/missing/rank").statusCode()).isEqualTo(404);
    }

    @Test
    @Order(2)
    void duplicateLateAndOutOfOrderEvents() throws Exception {
        retryEvent = event("dedup", 50, Instant.now());
        post(retryEvent);
        post(retryEvent);
        post(event("dedup", 30, Instant.now().minusSeconds(60)));
        post(event("dedup", 20, Instant.now().minusSeconds(30)));
        post(event("dedup", 1000, Instant.now().minusSeconds(121)));
        await()
                .atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                assertThat(result("/players/dedup/rank").players().getFirst().score())
                                        .isEqualTo(100));
        var future = event("future", 1, Instant.now().plusSeconds(60));
        assertThat(send(future).statusCode()).isEqualTo(400);
        var invalid = event("invalid", -1, Instant.now());
        assertThat(send(invalid).statusCode()).isEqualTo(400);
    }

    @Test
    @Order(3)
    void tiesAreStableAcrossPartitions() throws Exception {
        post(event("tie-a", 99, Instant.now()));
        post(event("tie-z", 99, Instant.now()));
        await()
                .atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                assertThat(result("/players/tie-a/rank").players().getFirst().rank())
                                        .isEqualTo(result("/players/tie-z/rank").players().getFirst().rank() + 1));
    }

    @Test
    @Order(4)
    void redisCompleteLossRebuildsFromCurrentStreamsState() throws Exception {
        projector.close(Duration.ofSeconds(30));
        redis.getConnectionFactory().getConnection().serverCommands().flushAll();
        assertThat(get("/top").statusCode()).isEqualTo(503);
        projector = projector("redis-loss");
        projector.start();
        await()
                .atMost(Duration.ofSeconds(90))
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            assertThat(result("/players/dedup/rank").players().getFirst().score()).isEqualTo(100);
                            assertThat(result("/top").totalPlayers()).isEqualTo(18);
                        });
        redis.getConnectionFactory().getConnection().serverCommands().flushAll();
        await()
                .atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .untilAsserted(() -> assertThat(result("/top").totalPlayers()).isEqualTo(18));
    }

    @Test
    @Order(5)
    void bothTopologiesRestoreFromChangelogsWithoutLocalFiles() throws Exception {
        closeStreams();
        redis.getConnectionFactory().getConnection().serverCommands().flushAll();
        assertThat(get("/top").statusCode()).isEqualTo(503);
        startStreams("fresh-directories");
        await()
                .atMost(Duration.ofSeconds(90))
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                assertThat(result("/players/dedup/rank").players().getFirst().score())
                                        .isEqualTo(100));
        post(retryEvent);
        await()
                .during(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(15))
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                assertThat(result("/players/dedup/rank").players().getFirst().score())
                                        .isEqualTo(100));
    }

    @Test
    @Order(6)
    void expirationRunsInRealKafkaWithoutWaitingTenMinutes() {
        CLOCK.offset.set(Duration.ofMinutes(11).toMillis());
        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () -> {
                            long members =
                                    IntStream.range(0, PARTITIONS)
                                            .mapToLong(p -> redis.opsForZSet().zCard("player-ranking:10m:" + p))
                                            .sum();
                            assertThat(members).isZero();
                        });
    }

    @Test
    @Order(7)
    void committedAuditPersistsIdempotentlyInPostgres() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "audit-test");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        var summaries = new ArrayList<String>();
        try (var consumer =
                     new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("ranking-audit-v1"));
            await()
                    .atMost(Duration.ofSeconds(30))
                    .until(
                            () -> {
                                consumer
                                        .poll(Duration.ofMillis(300))
                                        .forEach(record -> summaries.add(record.value()));
                                return summaries.stream()
                                        .map(value -> Json.read(value, AuditSummary.class))
                                        .mapToLong(AuditSummary::accepted)
                                        .sum()
                                        == 20;
                            });
        }
        var persistence = new AuditPersistence(jdbc);
        persistence.persist(summaries);
        persistence.persist(summaries);
        assertThat(jdbc.queryForObject("SELECT sum(accepted) FROM score_processing_audit", Long.class))
                .isEqualTo(20);
        assertThat(
                jdbc.queryForObject("SELECT sum(duplicates) FROM score_processing_audit", Long.class))
                .isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT sum(rejected) FROM score_processing_audit", Long.class))
                .isEqualTo(1);
    }

    @Test
    @Order(8)
    void productionStreamApplicationStartsWithFlywayAndAuditListener() throws Exception {
        closeStreams();
        stream =
                startWeb(
                        ro.midra.stream.Application.class,
                        "ranking-stream-service",
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--ranking.replication-factor=1",
                        "--ranking.threads=1",
                        "--ranking.state-dir=" + directory.resolve("production"));
        post(event("production-runtime", 777, Instant.now()));
        await()
                .atMost(Duration.ofSeconds(90))
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                assertThat(result("/players/production-runtime/rank").players().getFirst().score())
                                        .isEqualTo(777));
        assertThat(stream.getBean(StreamRuntime.class).health().getStatus().getCode()).isEqualTo("UP");
    }

    private static void createTopics() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin
                    .createTopics(
                            List.of(
                                    new NewTopic("score-events-v1", PARTITIONS, (short) 1),
                                    new NewTopic("ranking-totals-v1", PARTITIONS, (short) 1)
                                            .configs(Map.of("cleanup.policy", "compact")),
                                    new NewTopic("ranking-audit-v1", PARTITIONS, (short) 1)))
                    .all()
                    .get();
        }
    }

    private static void startStreams(String folder) {
        calculator =
                new KafkaStreams(
                        RankingTopology.build(
                                CLOCK,
                                WindowPolicy.standard(),
                                "score-events-v1",
                                "ranking-totals-v1",
                                "ranking-audit-v1"),
                        properties("calculator", folder));
        projector = projector(folder);
        calculator.start();
        projector.start();
    }

    private static KafkaStreams projector(String folder) {
        return new KafkaStreams(
                ProjectionTopology.build("ranking-totals-v1", redis, new SimpleMeterRegistry()),
                properties("projection", folder));
    }

    private static Properties properties(String name, String folder) {
        return StreamRuntime.properties(
                KAFKA.getBootstrapServers(),
                directory.resolve(folder).resolve(name).toString(),
                1,
                1,
                "it-" + name);
    }

    private static void closeStreams() {
        if (calculator != null) calculator.close(Duration.ofSeconds(30));
        if (projector != null) projector.close(Duration.ofSeconds(30));
    }

    private static ConfigurableApplicationContext startWeb(
            Class<?> application, String module, String... extra) {
        var args =
                new ArrayList<String>(
                        List.of(
                                "--spring.config.location=file:"
                                        + Path.of("..", module, "src/main/resources/application.yml").toAbsolutePath(),
                                "--server.port=0",
                                "--spring.main.banner-mode=off",
                                "--logging.level.root=WARN",
                                "--spring.cloud.gateway.server.webmvc.enabled=" + module.equals("api-gateway"),
                                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                                "--spring.data.redis.host=" + REDIS.getHost(),
                                "--spring.data.redis.port=" + REDIS.getMappedPort(6379),
                                "--ranking.partitions=" + PARTITIONS));
        if (!module.equals("ranking-stream-service"))
            args.add(
                    "--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration");
        args.addAll(List.of(extra));
        return new SpringApplicationBuilder(application).run(args.toArray(String[]::new));
    }

    private static int port(ConfigurableApplicationContext context) {
        return ((ServletWebServerApplicationContext) context).getWebServer().getPort();
    }

    private static ScoreEvent event(String player, long points, Instant timestamp) {
        return new ScoreEvent(UUID.randomUUID(), player, points, timestamp, Instant.now());
    }

    private static void post(ScoreEvent event) throws Exception {
        var response = send(event);
        assertThat(response.statusCode()).withFailMessage(response.body()).isEqualTo(202);
        assertThat(response.headers().firstValue("X-Request-ID")).contains("integration-request");
    }

    private static HttpResponse<String> send(ScoreEvent event) throws Exception {
        var request =
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/scores"))
                        .header("Content-Type", "application/json")
                        .header("X-Request-ID", "integration-request")
                        .POST(HttpRequest.BodyPublishers.ofString(Json.write(event)))
                        .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return HTTP.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/rankings" + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static RankingResult result(String path) throws Exception {
        var response = get(path);
        assertThat(response.statusCode()).withFailMessage(response.body()).isEqualTo(200);
        return Json.read(response.body(), RankingResult.class);
    }

    static final class AdjustableClock extends Clock {
        final AtomicLong offset = new AtomicLong();

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.now().plusMillis(offset.get());
        }
    }
}
