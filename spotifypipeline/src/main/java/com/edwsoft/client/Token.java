package com.edwsoft.client;


import java.time.Instant;

public record Token(String token, Instant expiresAt) {
    private static final int SECURITY_BUFFER  = 60;
    boolean isValid() {
        return !token.isEmpty() && Instant.now().isBefore(expiresAt.minusSeconds(SECURITY_BUFFER));
    }
}
