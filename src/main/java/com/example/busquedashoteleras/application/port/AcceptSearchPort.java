package com.example.busquedashoteleras.application.port;

import com.example.busquedashoteleras.domain.SearchSubmission;

/**
 * Registra de forma duradera la solicitud y su publicacion pendiente en una transaccion.
 * El retorno normal exige que el adaptador haya confirmado el commit; no implica
 * publicacion en Kafka ni procesamiento. Se invoca sin una transaccion externa.
 * Ante un fallo, propaga la excepcion y no comunica una aceptacion confirmada.
 */
@FunctionalInterface
public interface AcceptSearchPort {
    void accept(SearchSubmission submission);
}
