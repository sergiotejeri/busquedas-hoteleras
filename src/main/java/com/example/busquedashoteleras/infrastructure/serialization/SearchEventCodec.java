package com.example.busquedashoteleras.infrastructure.serialization;

import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.time.LocalDate;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Locale;
import tools.jackson.core.JacksonException;

public final class SearchEventCodec {
    private static final int SCHEMA_VERSION = 1;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter
            .ofPattern("dd/MM/uuuu", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    public byte[] encode(SearchSubmission submission) {
        SearchCriteria criteria = submission.criteria();
        SearchEvent event = new SearchEvent(SCHEMA_VERSION, submission.searchId(), criteria.hotelId(),
                DATE_FORMAT.format(criteria.checkIn()), DATE_FORMAT.format(criteria.checkOut()), criteria.ages());
        return SearchJson.MAPPER.writeValueAsBytes(event);
    }

    public SearchSubmission decode(String rawJson) {
        try {
            SearchEvent event = SearchJson.MAPPER.readValue(rawJson, SearchEvent.class);
            if (event == null) {
                throw new InvalidSearchEventException("INVALID_EVENT", "Search event payload is invalid", null);
            }
            if (event.schemaVersion() != SCHEMA_VERSION) {
                throw new InvalidSearchEventException("UNSUPPORTED_SCHEMA", "Unsupported event schema version", null);
            }
            SearchCriteria criteria = new SearchCriteria(event.hotelId(),
                    LocalDate.parse(event.checkIn(), DATE_FORMAT), LocalDate.parse(event.checkOut(), DATE_FORMAT),
                    event.ages());
            return new SearchSubmission(event.searchId(), criteria);
        } catch (JacksonException | IllegalArgumentException | DateTimeException exception) {
            throw new InvalidSearchEventException("INVALID_EVENT", "Search event payload is invalid", exception);
        }
    }
}
