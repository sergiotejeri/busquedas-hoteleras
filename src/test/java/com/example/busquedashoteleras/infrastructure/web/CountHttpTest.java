package com.example.busquedashoteleras.infrastructure.web;

import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.busquedashoteleras.application.port.SearchQueryPort;
import com.example.busquedashoteleras.application.service.GetSearchCountService;
import com.example.busquedashoteleras.domain.CountResult;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchStatus;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(CountController.class)
@Import(CountHttpTest.UseCase.class)
class CountHttpTest {
    private static final UUID ID = UUID.fromString("c40c839d-e50d-4805-966c-6376baa01d33");
    private static final String COUNT = "/count";
    private static final String SEARCH_ID = "searchId";
    private static final String ERROR_CODE = "$.code";
    @Autowired
    private MockMvc mvc;
    @MockitoBean
    private SearchQueryPort port;

    @Test
    void returnsOriginalCriteriaAndLongCount() throws Exception {
        var criteria = new SearchCriteria("hotel", LocalDate.of(2024, 2, 28), LocalDate.of(2024, 2, 29), List.of(30, 1, 30));
        when(port.find(ID)).thenReturn(Optional.of(new CountResult(ID, SearchStatus.PROCESSED, criteria, 3_000_000_000L)));
        mvc.perform(get(COUNT).param(SEARCH_ID, ID.toString()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.searchId").value(ID.toString()))
                .andExpect(jsonPath("$.search.hotelId").value("hotel"))
                .andExpect(jsonPath("$.search.checkIn").value("28/02/2024"))
                .andExpect(jsonPath("$.search.checkOut").value("29/02/2024"))
                .andExpect(jsonPath("$.search.ages").value(org.hamcrest.Matchers.contains(30, 1, 30)))
                .andExpect(jsonPath("$.count").value(3_000_000_000L))
                .andExpect(jsonPath("$.status").doesNotExist());
        verify(port).find(ID);
    }

    @Test
    void pendingHasRetryHintAndNoInventedCount() throws Exception {
        when(port.find(ID)).thenReturn(Optional.of(new CountResult(ID, SearchStatus.PENDING, null, null)));
        mvc.perform(get(COUNT).param(SEARCH_ID, ID.toString())).andExpect(status().isAccepted())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.searchId").value(ID.toString()))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.count").doesNotExist()).andExpect(jsonPath("$.search").doesNotExist());
        verify(port).find(ID);
    }

    @Test
    void missingSearchIsNotFound() throws Exception {
        when(port.find(ID)).thenReturn(Optional.empty());
        mvc.perform(get(COUNT).param(SEARCH_ID, ID.toString())).andExpect(status().isNotFound())
                .andExpect(jsonPath(ERROR_CODE).value("SEARCH_NOT_FOUND"));
        verify(port).find(ID);
    }

    @Test
    void failedSearchIsExplicitAndDoesNotSuggestPendingWork() throws Exception {
        when(port.find(ID)).thenReturn(Optional.of(new CountResult(ID, SearchStatus.FAILED, null, null)));
        mvc.perform(get(COUNT).param(SEARCH_ID, ID.toString())).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath(ERROR_CODE).value("SEARCH_PROCESSING_FAILED"))
                .andExpect(header().doesNotExist("Retry-After")).andExpect(jsonPath("$.count").doesNotExist());
        verify(port).find(ID);
    }

    @Test
    void lostRecoverableConnectionReturnsSafeUnavailableResponse() throws Exception {
        when(port.find(ID)).thenThrow(new RecoverableDataAccessException("internal Oracle connection details"));
        mvc.perform(get(COUNT).param(SEARCH_ID, ID.toString())).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath(ERROR_CODE).value("STORAGE_UNAVAILABLE"))
                .andExpect(jsonPath("$.message").value("Search could not be confirmed. Try again later."));
        verify(port).find(ID);
    }

    @Test
    void unavailableStorageIsNotReportedAsMissing() throws Exception {
        when(port.find(ID)).thenThrow(new DataAccessResourceFailureException("internal connection details"));
        mvc.perform(get(COUNT).param(SEARCH_ID, ID.toString())).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath(ERROR_CODE).value("STORAGE_UNAVAILABLE"))
                .andExpect(jsonPath("$.message").value("Search could not be confirmed. Try again later."));
        verify(port).find(ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "?searchId=", "?searchId=invalid", "?searchId=1-1-1-1-1",
            "?searchId=C40C839D-E50D-4805-966C-6376BAA01D33", "?other=value",
            "?searchId=c40c839d-e50d-4805-966c-6376baa01d33&searchId=c40c839d-e50d-4805-966c-6376baa01d33",
            "?searchId=c40c839d-e50d-4805-966c-6376baa01d33&extra=value"})
    void rejectsAmbiguousOrNonCanonicalQueryBeforeAccessingStorage(String query) throws Exception {
        mvc.perform(get(COUNT + query)).andExpect(status().isBadRequest())
                .andExpect(jsonPath(ERROR_CODE).value("INVALID_SEARCH_ID"))
                .andExpect(jsonPath("$.field").value(SEARCH_ID));
        verifyNoInteractions(port);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class UseCase {
        @Bean
        GetSearchCountService countService(SearchQueryPort port) {
            return new GetSearchCountService(port);
        }
    }
}
