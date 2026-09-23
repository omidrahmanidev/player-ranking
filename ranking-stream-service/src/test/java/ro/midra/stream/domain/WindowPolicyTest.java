package ro.midra.stream.domain;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WindowPolicyTest {
    @Test
    void retentionMustCoverWindowAndGrace() {
        assertThatThrownBy(
                () ->
                        new WindowPolicy(
                                Duration.ofMinutes(10), Duration.ofMinutes(2), Duration.ofMinutes(11)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void scoresMustRemainExactlyRepresentableInRedis() {
        assertThatThrownBy(() -> new PlayerTotal(9_007_199_254_740_991L, 1).add(1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
