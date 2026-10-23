package com.example.busquedashoteleras.application.port;

import java.util.UUID;

public record OutboxClaim(UUID searchId, UUID token, String eventJson, int retryAttempts) { }
