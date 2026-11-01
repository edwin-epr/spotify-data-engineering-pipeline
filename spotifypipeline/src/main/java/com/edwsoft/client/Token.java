package com.edwsoft.client;


import java.time.Instant;
import java.util.Objects;

public record Token(String token, Instant expiresAt) {
    private static final int SECURITY_BUFFER = 60;
    boolean isValid() {
        return Objects.nonNull(token)
                && !token.isEmpty()
                && Objects.nonNull(expiresAt)
                && Instant.now().isBefore(expiresAt.minusSeconds(SECURITY_BUFFER));
    }
}
