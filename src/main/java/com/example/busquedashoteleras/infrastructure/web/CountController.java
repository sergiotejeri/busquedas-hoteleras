package com.example.busquedashoteleras.infrastructure.web;

import com.example.busquedashoteleras.application.service.GetSearchCountService;
import com.example.busquedashoteleras.domain.CountResult;
import com.example.busquedashoteleras.domain.SearchStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class CountController {
    private static final String SEARCH_ID = "searchId";
    private final GetSearchCountService service;

    public CountController(GetSearchCountService service) {
        this.service = service;
    }

    @GetMapping(value = "/count", produces = "application/json")
    @Operation(summary = "Consulta el conteo de una busqueda aceptada",
            description = "Conserva el orden de edades. El estado pendiente y Retry-After son decisiones del contrato asincrono.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Busqueda procesada; conteo incluye la propia busqueda",
                content = @Content(schema = @Schema(implementation = CountResponse.class))),
        @ApiResponse(responseCode = "202", description = "Pendiente; Retry-After: 1 sugiere reconsulta, sin plazo garantizado",
                content = @Content(schema = @Schema(implementation = PendingResponse.class))),
        @ApiResponse(responseCode = "400", description = "Se requiere un unico searchId UUID canonico en minusculas, sin otros parametros",
                content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ApiError.class))),
        @ApiResponse(responseCode = "404", description = "Busqueda inexistente",
                content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ApiError.class))),
        @ApiResponse(responseCode = "503", description = "Procesamiento fallido o almacenamiento no disponible",
                content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ApiError.class)))
    })
    public ResponseEntity<?> count(
            @Parameter(name = SEARCH_ID, in = ParameterIn.QUERY, required = true,
                    schema = @Schema(type = "string", format = "uuid"), description = "ID devuelto por POST /search")
            @RequestParam MultiValueMap<String, String> parameters) {
        List<String> ids = parameters.get(SEARCH_ID);
        if (parameters.size() != 1 || ids == null || ids.size() != 1) {
            return invalidId();
        }
        UUID id;
        try {
            id = UUID.fromString(ids.getFirst());
        } catch (IllegalArgumentException invalid) {
            return invalidId();
        }
        if (!id.toString().equals(ids.getFirst())) {
            return invalidId();
        }
        return service.find(id).map(CountController::response).orElseGet(() -> ResponseEntity.status(404)
                .body(new ApiExceptionHandler.ApiError("SEARCH_NOT_FOUND", SEARCH_ID, "Search was not found.")));
    }

    private static ResponseEntity<?> response(CountResult result) {
        return switch (result.status()) {
            case PENDING -> ResponseEntity.accepted().header("Retry-After", "1")
                    .body(new PendingResponse(result.searchId(), SearchStatus.PENDING));
            case FAILED -> ResponseEntity.status(503).body(new ApiExceptionHandler.ApiError(
                    "SEARCH_PROCESSING_FAILED", null, "Search processing failed."));
            case PROCESSED -> ResponseEntity.ok(new CountResponse(result.searchId(),
                    SearchDetailsResponse.from(result.search()), result.count()));
        };
    }

    private static ResponseEntity<ApiExceptionHandler.ApiError> invalidId() {
        return ResponseEntity.badRequest().body(new ApiExceptionHandler.ApiError("INVALID_SEARCH_ID", SEARCH_ID,
                "Exactly one canonical lowercase searchId UUID is required, without other parameters."));
    }

    public record CountResponse(UUID searchId, SearchDetailsResponse search,
            @Schema(type = "integer", format = "int64", minimum = "1") long count) { }

    public record PendingResponse(UUID searchId, @Schema(allowableValues = "PENDING") SearchStatus status) { }
}
