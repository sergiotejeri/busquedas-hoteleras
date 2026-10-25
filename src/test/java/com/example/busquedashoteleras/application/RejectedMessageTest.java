package com.example.busquedashoteleras.application;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.busquedashoteleras.application.port.RejectedMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class RejectedMessageTest {
    private static final String INVALID_REASON = "INVALID_EVENT";
    private static final String TOPIC = "topic";
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void requiresTopic(String topic) {
        assertThrows(IllegalArgumentException.class, () -> new RejectedMessage(topic, 0, 0, null, null, INVALID_REASON));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void requiresReason(String reason) {
        assertThrows(IllegalArgumentException.class, () -> new RejectedMessage(TOPIC, 0, 0, null, null, reason));
    }

    @Test
    void rejectsInvalidCoordinatesAndOversizedMetadata() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> new RejectedMessage(TOPIC, -1, 0, null, null, INVALID_REASON)),
                () -> assertThrows(IllegalArgumentException.class, () -> new RejectedMessage(TOPIC, 0, -1, null, null, INVALID_REASON)),
                () -> assertThrows(IllegalArgumentException.class, () -> new RejectedMessage("t".repeat(250), 0, 0, null, null, INVALID_REASON)),
                () -> assertThrows(IllegalArgumentException.class, () -> new RejectedMessage(TOPIC, 0, 0, null, null, "r".repeat(81))));
    }

    @Test
    void acceptsBoundaryCoordinatesAndMissingPayload() {
        var message = new RejectedMessage("t".repeat(249), Integer.MAX_VALUE, Long.MAX_VALUE, null, null, "r".repeat(80));
        assertAll(
                () -> assertEquals(Integer.MAX_VALUE, message.partition(), "Conserva la particion"),
                () -> assertEquals(Long.MAX_VALUE, message.offset(), "El offset usa 64 bits"),
                () -> assertNull(message.rawPayload(), "Un tombstone no tiene contenido"),
                () -> assertNull(message.eventKey(), "Una clave ausente tambien se puede aislar"));
    }
}
