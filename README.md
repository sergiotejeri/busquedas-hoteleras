# Búsquedas hoteleras

Servicio de búsquedas hoteleras creado con [Spring Initializr](https://start.spring.io/). `POST /search` valida y guarda solicitudes en Oracle.

## Arranque

Necesitas Docker con Compose y contenedores Linux. La compilación se hace dentro de Docker; no hace falta instalar Java ni Maven. La primera construcción necesita acceso a Internet.

```shell
docker compose up -d --build app topic-init
```

El primer arranque puede tardar varios minutos. La aplicación escucha en `http://localhost:8080`; la ruta `/` devuelve 404. Para detener los servicios conservando los datos:

```shell
docker compose stop
```

## Registrar una búsqueda

```shell
curl -i http://localhost:8080/search -H "Content-Type: application/json" -d '{"hotelId":"hotel-1","checkIn":"01/12/2026","checkOut":"03/12/2026","ages":[30,1]}'
```

Devuelve `202` con `{"searchId":"UUID"}` después de guardar la solicitud y su evento pendiente en una misma transacción. Cada solicitud recibe un ID distinto, aunque sus criterios coincidan. El `202` es una decisión del contrato y confirma la aceptación para procesamiento asíncrono.

El hotel no puede estar vacío, la entrada debe ser anterior a la salida y las edades deben ser enteros no negativos en una lista no vacía. Todos los campos son obligatorios y las fechas usan `dd/MM/yyyy`. Se rechazan propiedades desconocidas, duplicadas y conversiones implícitas de tipos.

Los datos inválidos devuelven `400` con código, campo y mensaje. El cuerpo admite hasta 1 MiB, también sin `Content-Length`; el evento serializado, hasta 262.144 bytes UTF-8. Son límites técnicos: superarlos devuelve un error global `REQUEST_TOO_LARGE`. Si no se puede obtener conexión, devuelve `503`. Una confirmación perdida puede dejar un resultado incierto: repetir el POST crea otra solicitud.

## Consultar el conteo

```shell
curl -i "http://localhost:8080/count?searchId=UUID"
```

Usa el ID devuelto por POST. Devuelve `200` con `searchId`, `search` y `count`; `202` si sigue pendiente, `404` si no existe y `503` ante fallo de procesamiento o almacenamiento. El `202` pendiente y `Retry-After: 1` son decisiones del contrato; no garantizan cuándo terminará. Se exige un único UUID canónico en minúsculas, sin parámetros adicionales; un error devuelve `400`.

Swagger: `http://localhost:8080/swagger-ui.html`. OpenAPI: `http://localhost:8080/v3/api-docs`.

Salud: `/actuator/health/liveness` comprueba el proceso y `/actuator/health/readiness` añade Oracle. Si Oracle falla, readiness devuelve `503`; Kafka puede recuperarse mediante la outbox. No se exponen detalles internos. La conexión al pool y la negociación con Oracle tienen límites de 3 s; el apagado dispone de 45 s por fase y Docker espera hasta 60 s.

## Decisiones de diseño

- Arquitectura hexagonal: dominio independiente de frameworks, aplicación dependiente del dominio y adaptadores en infraestructura. Los objetos del dominio son inmutables.
- La igualdad conserva el identificador del hotel, las fechas y el orden y las repeticiones de edades. `[30, 1]` y `[1, 30]` son distintos. Es la interpretación adoptada ante la contradicción entre el texto y el ejemplo del enunciado.
- Los grupos se buscan por hash y se comparan por su contenido completo, conservando el orden de las edades.
- El procesamiento guarda la búsqueda y su estado en una transacción. Repetir el mismo evento no duplica la búsqueda; los criterios deben coincidir con la solicitud aceptada.
- El conteo obtiene el estado, los criterios y el total en una sola consulta a Oracle. Solo cuenta búsquedas procesadas del mismo grupo.
- La cuarentena guarda el mensaje rechazado y su motivo, sin duplicar reentregas ni cambiar búsquedas aceptadas.
- La solicitud y el evento se guardan juntos en Oracle. El evento tiene un contrato JSON versionado; los criterios almacenados usan fechas ISO y una representación determinista.
- La outbox reserva los eventos de uno en uno y confirma solo con el token vigente. El servicio admite hasta 100 por ciclo; ante un fallo transitorio aplaza el evento entre 1 y 30 segundos y detiene el ciclo. Un rechazo permanente se guarda sin degradar búsquedas ya procesadas.
- El productor envía el JSON original a `hotel_availability_searches`, con el ID como clave, y espera la confirmación de Kafka. Un timeout deja el envío recuperable: puede haberse recibido y volver a entregarse.
- El envío se ejecuta en segundo plano, con un segundo entre ciclos y hasta 100 eventos por lote. Si Kafka falla, se reintenta. También se reenvían eventos confirmados hace al menos cinco minutos si la búsqueda sigue pendiente; se conserva el mismo ID.
- El consumidor persiste en hilos virtuales y confirma Kafka después del guardado. Si falla Oracle, reintenta sin descartar el mensaje.

## Pruebas y calidad

Para compilar y ejecutar las pruebas habituales y los controles:

```shell
docker build --progress=plain -t busquedas-hoteleras .
```

Para ejecutar también las pruebas con Oracle y Kafka reales:

```shell
docker compose --profile test build integration-test
docker compose up -d --wait oracle kafka
docker compose --profile test run --rm integration-test
```

Reconstruye el ejecutor cuando cambie el código. Los informes quedan en `test-results/`: `failsafe-reports/` para integración y `site/jacoco/index.html` para cobertura.

Las pruebas incluyen peticiones simultáneas que recorren HTTP, Kafka y Oracle, comprobando IDs únicos y el conteo de cada grupo.

También comprueban recuperación tras cortes bruscos de una JVM y expiración real de mensajes en un topic temporal. Estas pruebas pueden añadir varios minutos a la ejecución.

`verify` incluye ArchUnit, PMD, SpotBugs, CPD y JaCoCo. Comprueba dependencias entre capas, posibles errores y duplicación, y exige un 85 % de cobertura en líneas, ramas, métodos e instrucciones de bytecode, sin excluir clases. Un incumplimiento hace fallar la construcción. Las reglas están en `config/` y en el POM.

Para exportar los informes de la construcción:

```shell
docker build --target reports --output type=local,dest=target/quality .
```

La cobertura se consulta en `target/quality/jacoco/index.html`. Tanto `target/` como `test-results/` están excluidos de Git.

## Entorno

Java 21, Spring Boot 4.1.1 y Maven 3.10.0. Las imágenes usan JDK/JRE 25 y Oracle Database Free 23.26.3.0 Lite, fijados por digest.

Kafka y su cliente usan 4.4.0. El broker local tiene tres particiones y una réplica; conserva sus datos en `kafka-data` y no publica puertos. La imagen está fijada por digest y el topic se crea al arrancar `topic-init`.

Compose separa los usuarios de aplicación y pruebas. Las contraseñas son de desarrollo local y Oracle no publica puertos. El volumen `oracle-data` conserva los datos entre arranques.

Flyway aplica las migraciones al iniciar: V1 crea las tablas, V2 añade las reservas y V3 conserva el nivel de reintento, V4 añade los grupos de criterios, V5 las búsquedas procesadas y V6 la cuarentena. `clean` está desactivado; los cambios de esquema se añaden mediante nuevas migraciones.
