package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.busquedashoteleras.application.port.AcceptSearchPort;
import com.example.busquedashoteleras.application.service.AcceptSearchService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.CannotCreateTransactionException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@WebMvcTest(SearchController.class)
@Import({SearchHttpTest.UseCase.class, JsonInputConfiguration.class})
class SearchHttpTest {
    private static final String ERROR_CODE = "$.code";
    private static final String ERROR_FIELD = "$.field";
    private static final String SEARCH_PATH = "/search";
    private static final String JSON_TYPE = "application/json";
    private static final UUID ID = UUID.fromString("c40c839d-e50d-4805-966c-6376baa01d33");
    private static final String VALID = "{\"hotelId\":\"hotel\",\"checkIn\":\"28/02/2024\","
            + "\"checkOut\":\"29/02/2024\",\"ages\":[30,0,30]}";

    @Autowired
    private MockMvc mvc;
    @MockitoBean
    private AcceptSearchPort port;

    @Test
    void acceptsValidatedCriteriaAndReturnsIdentifier() throws Exception {
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(VALID))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.searchId").value(ID.toString()));
        verify(port).accept(new SearchSubmission(ID, new SearchCriteria("hotel", LocalDate.of(2024, 2, 28),
                LocalDate.of(2024, 2, 29), List.of(30, 0, 30))));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{}", "null", "[]", "", "{",
            "{\"hotelId\":null}",
            "{\"hotelId\":\"hotel\",\"checkIn\":\"01/01/2024\",\"checkOut\":\"01/01/2024\",\"ages\":[1]}",
            "{\"hotelId\":\"hotel\",\"checkIn\":\"02/01/2024\",\"checkOut\":\"01/01/2024\",\"ages\":[1]}"
    })
    void rejectsMissingOrInvalidCriteria(String body) throws Exception {
        assertRejected(body);
    }

    @ParameterizedTest
    @ValueSource(strings = {"31/02/2024", "29/02/2023", "2024-02-28", "1/02/2024", "", " 28/02/2024"})
    void rejectsInvalidDateFormats(String date) throws Exception {
        assertRejected(VALID.replace("28/02/2024", date));
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "[]", "[null]", "[-1]", "[1.5]", "[\"1\"]", "[true]", "[2147483648]", "{}"})
    void rejectsInvalidAges(String ages) throws Exception {
        assertRejected(VALID.replace("[30,0,30]", ages));
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "123", "true", "\"\"", "\"   \"", "[]", "{}"})
    void rejectsInvalidHotel(String hotel) throws Exception {
        assertRejected(VALID.replace("\"hotel\"", hotel));
    }

    @ParameterizedTest
    @ValueSource(strings = {"hotelId", "checkIn", "checkOut", "ages"})
    void identifiesNullAndMissingFields(String field) throws Exception {
        ObjectNode body = (ObjectNode) JsonMapper.builder().build().readTree(VALID);
        body.putNull(field);
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(body.toString()))
                .andExpect(status().isBadRequest()).andExpect(jsonPath(ERROR_FIELD).value(field));
        body.remove(field);
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(body.toString()))
                .andExpect(status().isBadRequest()).andExpect(jsonPath(ERROR_FIELD).value(field));
        verifyNoInteractions(port);
    }

    @Test
    void rejectsUnknownDuplicateAndTrailingContent() {
        assertAll(
                () -> assertRejected(VALID.replace("\"ages\":", "\"extra\":1,\"ages\":")),
                () -> assertRejected(VALID.replace("\"ages\":", "\"hotelId\":\"other\",\"ages\":")),
                () -> assertRejected(VALID + "{}"));
    }

    @Test
    void doesNotReportAcceptanceWhenStorageFails() {
        doThrow(new CannotCreateTransactionException("private connection details"))
                .when(port).accept(org.mockito.ArgumentMatchers.any());
        assertAll(() -> mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(VALID))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath(ERROR_CODE).value("STORAGE_UNAVAILABLE"))
                .andExpect(jsonPath("$.message").value("Search could not be confirmed. Try again later.")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"checkIn", "checkOut"})
    void rejectsYearZeroWithFieldError(String field) throws Exception {
        ObjectNode body = (ObjectNode) JsonMapper.builder().build().readTree(VALID);
        body.put(field, "01/01/0000");
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(body.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath(ERROR_CODE).value("INVALID_DATE"))
                .andExpect(jsonPath(ERROR_FIELD).value(field));
        verifyNoInteractions(port);
    }

    @ParameterizedTest
    @CsvSource({"01/01/0001,02/01/0001", "30/12/9999,31/12/9999", "31/12/2026,01/01/2027"})
    void acceptsSupportedDateBoundaries(String checkIn, String checkOut) throws Exception {
        ObjectNode body = (ObjectNode) JsonMapper.builder().build().readTree(VALID);
        body.put("checkIn", checkIn);
        body.put("checkOut", checkOut);
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(body.toString()))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.searchId").value(ID.toString()));
        var formatter = java.time.format.DateTimeFormatter.ofPattern("dd/MM/uuuu");
        verify(port).accept(new SearchSubmission(ID, new SearchCriteria("hotel",
                LocalDate.parse(checkIn, formatter), LocalDate.parse(checkOut, formatter), List.of(30, 0, 30))));
    }
    @ParameterizedTest
    @ValueSource(strings = {"\\ud800", "\\udc00", "\\ud800x"})
    void rejectsMalformedUnicodeBeforeAcceptance(String escapedHotel) throws Exception {
        String body = VALID.replace("\"hotel\"", "\"hotel-" + escapedHotel + "\"");
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath(ERROR_CODE).value("INVALID_HOTEL_ID"))
                .andExpect(jsonPath(ERROR_FIELD).value("hotelId"));
        verifyNoInteractions(port);
    }
    private void assertRejected(String body) throws Exception {
        mvc.perform(post(SEARCH_PATH).contentType(JSON_TYPE).content(body))
                .andExpect(status().isBadRequest()).andExpect(jsonPath(ERROR_CODE).isNotEmpty())
                .andExpect(jsonPath("$.message").isNotEmpty());
        verifyNoInteractions(port);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class UseCase {
        @Bean
        AcceptSearchService acceptSearchService(AcceptSearchPort port) {
            return new AcceptSearchService(port, () -> ID);
        }
    }
}
