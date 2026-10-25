package com.example.busquedashoteleras.infrastructure.web;

import com.example.busquedashoteleras.application.service.AcceptSearchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class SearchController {
    private final AcceptSearchService service;

    public SearchController(AcceptSearchService service) {
        this.service = service;
    }

    @PostMapping(value = "/search", consumes = "application/json", produces = "application/json")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(summary = "Acepta una busqueda para procesamiento asincrono",
            description = "Campos obligatorios; fechas dd/MM/yyyy y entrada anterior a salida; edades enteras no negativas. "
                    + "Rechaza propiedades desconocidas y duplicadas. El cuerpo admite hasta 1048576 bytes; "
                    + "el evento serializado admite hasta 262144 bytes UTF-8. El exceso devuelve 400 REQUEST_TOO_LARGE, sin campo.")
    @ApiResponses({
        @ApiResponse(responseCode = "202", description = "Solicitud y evento guardados; ID nuevo en cada peticion",
                content = @Content(schema = @Schema(implementation = AcceptedSearch.class))),
        @ApiResponse(responseCode = "400", description = "Datos invalidos",
                content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ApiError.class))),
        @ApiResponse(responseCode = "503", description = "Almacenamiento no disponible",
                content = @Content(schema = @Schema(implementation = ApiExceptionHandler.ApiError.class)))
    })
    public AcceptedSearch accept(@RequestBody SearchRequest request) {
        return new AcceptedSearch(service.accept(request.toCriteria()));
    }

    public record AcceptedSearch(UUID searchId) { }
}
