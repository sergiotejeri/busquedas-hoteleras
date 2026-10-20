# Búsquedas hoteleras

Servicio para registrar búsquedas hoteleras y consultar cuántas coinciden con sus criterios.

## Estado

Base generada con [Spring Initializr](https://start.spring.io/): Maven, Java 21 y Spring Web MVC. Incluye la clase principal y una prueba de carga del contexto. Todavía no hay endpoints de negocio ni conexiones a Oracle o Kafka.

## Construir y comprobar

Se necesita Docker con contenedores Linux y acceso a Internet en la primera construcción. No es necesario instalar Java ni Maven en el equipo.

```shell
docker build --progress=plain -t busquedas-hoteleras .
```

La construcción ejecuta `clean verify`, incluida la prueba del contexto, y genera la imagen ejecutable. La prueba de contexto comprueba la configuración inicial, no reglas de negocio.

## Arrancar

```shell
docker run --rm --name busquedas-hoteleras -p 127.0.0.1:8080:8080 busquedas-hoteleras
```

El servidor escucha en `http://localhost:8080`. La ruta `/` devuelve 404 porque aún no hay controladores. Para detenerlo, pulsa Ctrl+C.

## Versiones

- Spring Boot 4.1.1 y Java 21 como nivel de compilación.
- Maven 3.10.0, fijado por el wrapper generado.
- JDK/JRE 25 en las imágenes Docker, fijadas por digest.

El POM descargado indicaba `4.1.1.RELEASE`; se ajustó a `4.1.1`, el identificador disponible en Maven Central.
