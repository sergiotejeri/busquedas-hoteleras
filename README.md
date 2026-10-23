# Búsquedas hoteleras

Servicio de búsquedas hoteleras creado con [Spring Initializr](https://start.spring.io/). Por ahora, `POST /search` valida y guarda solicitudes en Oracle. El conteo, Kafka y Swagger/OpenAPI están pendientes.

## Arranque

Necesitas Docker con Compose y contenedores Linux. La compilación se hace dentro de Docker; no hace falta instalar Java ni Maven. La primera construcción necesita acceso a Internet.

```shell
docker compose up -d --build app
```

El primer arranque puede tardar varios minutos. La aplicación escucha en `http://localhost:8080`; la ruta `/` devuelve 404. Para detener los servicios conservando los datos:

```shell
docker compose stop
```

## Registrar una búsqueda

```shell
curl -i http://localhost:8080/search -H "Content-Type: application/json" -d '{"hotelId":"hotel-1","checkIn":"01/12/2026","checkOut":"03/12/2026","ages":[30,1]}'
```

Devuelve `202` con `{"searchId":"UUID"}` después de guardar la solicitud y su evento pendiente en una misma transacción. Cada solicitud recibe un ID distinto, aunque sus criterios coincidan. El `202` es una decisión del contrato: la búsqueda está aceptada, todavía no procesada.

El hotel no puede estar vacío, la entrada debe ser anterior a la salida y las edades deben ser enteros no negativos en una lista no vacía. Todos los campos son obligatorios y las fechas usan `dd/MM/yyyy`. Se rechazan propiedades desconocidas, duplicadas y conversiones implícitas de tipos.

Los datos inválidos devuelven `400` con código, campo y mensaje. El evento serializado admite hasta 262.144 bytes UTF-8; superar ese límite produce un error global. Si no se puede obtener conexión, devuelve `503`. Una confirmación perdida puede dejar un resultado incierto: repetir el POST crea otra solicitud.

## Decisiones de diseño

- Arquitectura hexagonal: dominio independiente de frameworks, aplicación dependiente del dominio y adaptadores en infraestructura. Los objetos del dominio son inmutables.
- La igualdad conserva el identificador del hotel, las fechas y el orden y las repeticiones de edades. `[30, 1]` y `[1, 30]` son distintos. Es la interpretación adoptada ante la contradicción entre el texto y el ejemplo del enunciado.
- La solicitud y el evento se guardan juntos en Oracle. El evento tiene un contrato JSON versionado; los criterios almacenados usan fechas ISO y una representación determinista.
- La outbox permite reservar un evento con caducidad y un token nuevo. Una reserva vencida se puede recuperar; un token antiguo no puede confirmarla. La publicación automática se incorporará después.

## Pruebas y calidad

Para compilar y ejecutar las pruebas habituales y los controles:

```shell
docker build --progress=plain -t busquedas-hoteleras .
```

Para ejecutar también las pruebas con Oracle real:

```shell
docker compose --profile test build integration-test
docker compose up -d --wait oracle
docker compose --profile test run --rm integration-test
```

Reconstruye el ejecutor cuando cambie el código. Los informes quedan en `test-results/`: `failsafe-reports/` para integración y `site/jacoco/index.html` para cobertura.

`verify` incluye ArchUnit, PMD, SpotBugs, CPD y JaCoCo. Comprueba dependencias entre capas, posibles errores y duplicación, y exige un 85 % de cobertura en líneas, ramas, métodos e instrucciones de bytecode, sin excluir clases. Un incumplimiento hace fallar la construcción. Las reglas están en `config/` y en el POM.

Para exportar los informes de la construcción:

```shell
docker build --target reports --output type=local,dest=target/quality .
```

La cobertura se consulta en `target/quality/jacoco/index.html`. Tanto `target/` como `test-results/` están excluidos de Git.

## Entorno

Java 21, Spring Boot 4.1.1 y Maven 3.10.0. Las imágenes usan JDK/JRE 25 y Oracle Database Free 23.26.3.0 Lite, fijados por digest.

Compose separa los usuarios de aplicación y pruebas. Las contraseñas son de desarrollo local y Oracle no publica puertos. El volumen `oracle-data` conserva los datos entre arranques.

Flyway aplica las migraciones al iniciar: V1 crea las tablas y V2 añade las reservas de outbox. `clean` está desactivado; los cambios de esquema se añaden mediante nuevas migraciones.
