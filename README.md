# Búsquedas hoteleras

Servicio para registrar búsquedas hoteleras y consultar cuántas coinciden con sus criterios.

## Estado

Base generada con [Spring Initializr](https://start.spring.io/): Maven, Java 21 y Spring Web MVC. Incluye el modelo de criterios de búsqueda, sus pruebas unitarias y una prueba de carga del contexto. La persistencia en Oracle se prueba por integración. Todavía faltan los endpoints de negocio y Kafka.

## Criterios de búsqueda

`SearchCriteria` pertenece al dominio y solo depende de Java. Exige un hotel no nulo ni blanco, fechas no nulas con entrada anterior a salida y una lista no vacía de edades no nulas y mayores o iguales a cero. Una infracción produce `InvalidSearchException`, con código, campo y mensaje; su traducción a HTTP se incorporará con el adaptador web.

El record conserva una copia inmutable de las edades. No impone fechas futuras ni un máximo de edad adicional. Como decisión de implementación, la igualdad incluye todos los campos, conserva el orden y las repeticiones de edades y no normaliza el identificador del hotel. Así, `[30, 1]` y `[1, 30]` son criterios diferentes. Esta decisión hace explícita la interpretación adoptada ante la contradicción entre el texto y el ejemplo del enunciado.

## Solicitudes y resultados

`SearchSubmission` une un UUID obligatorio con los criterios validados. Recibe el identificador; su generación por solicitud corresponde al caso de uso de aceptación.

`SearchStatus` distingue `PENDING`, `PROCESSED` y `FAILED`. `CountResult` exige identificador y estado. Un resultado procesado incluye criterios y un conteo positivo (la propia búsqueda cuenta). Un resultado pendiente o fallido no expone criterios ni conteo: ausencia de resultado no equivale a cero. Estas invariantes se comprueban al construir los objetos.

Estos tipos solo modelan el contrato interno. Todavía no ejecutan transiciones, consultas ni procesamiento asíncrono, y no determinan por sí solos una respuesta HTTP.

## Aceptación de solicitudes

`AcceptSearchService` recibe criterios ya validados, solicita un UUID al generador inyectado y entrega una `SearchSubmission` al puerto `AcceptSearchPort`. Devuelve el ID únicamente después del retorno correcto del puerto; no deduplica solicitudes iguales ni oculta excepciones. Rechaza criterios, dependencias o identificadores generados nulos.

`OracleAcceptanceAdapter` guarda la solicitud y el evento pendiente en una transacción. Devuelve el control después del commit. Falta conectarlo al endpoint.

## Formato de eventos y criterios almacenados

`infrastructure.serialization` contiene los codecs JSON, sin filtrar dependencias de Jackson al dominio ni a la aplicación. Se reutiliza Jackson incluido en Spring Boot, con configuración propia para este contrato interno.

El evento v1 contiene `schemaVersion`, `searchId`, `hotelId`, `checkIn`, `checkOut` y `ages`. Se escribe en UTF-8, con fechas `dd/MM/uuuu` y orden de propiedades explícito. La lectura exige campos completos, tipos correctos, fechas reales y criterios válidos. Rechaza propiedades desconocidas o duplicadas, documentos concatenados y conversiones implícitas de cadenas o decimales a edades. Una versión desconocida produce `UNSUPPORTED_SCHEMA`; un contenido inválido, `INVALID_EVENT`, sin incluir el payload en el mensaje.

`CanonicalCriteriaCodec` representa únicamente los criterios con fechas ISO (`uuuu-MM-dd`) y orden de propiedades explícito. Conserva hotel, orden y repeticiones de edades, sin incorporar el ID ni la versión del evento. Esta es una representación interna determinista, no una implementación de un estándar general de JSON canónico. Un contenido almacenado inválido produce un error de estado.

Estos contratos son decisiones técnicas del proyecto. Todavía no hay productor ni consumidor de Kafka. La validación del formato no detecta una alteración que siga siendo válida: comprobar la coincidencia con la solicitud aceptada corresponderá al procesamiento. El límite de tamaño se comprueba al aceptar; la agrupación se incorporará con el procesamiento.

## Construir y comprobar

Se necesita Docker con contenedores Linux y acceso a Internet en la primera construcción. No es necesario instalar Java ni Maven en el equipo.

```shell
docker build --progress=plain -t busquedas-hoteleras .
```

La construcción ejecuta `clean verify`, incluidas las pruebas del dominio, la prueba de arranque y los controles de calidad, y genera la imagen ejecutable. Las pruebas del dominio comprueban validaciones, igualdad y copia defensiva; la prueba de arranque inicia el contexto a través de `main` y comprueba que queda activo.

## Controles de calidad

Los controles forman parte de `verify`: un incumplimiento hace fallar la construcción.

| Control | Alcance |
| --- | --- |
| ArchUnit 1.5.1 | Dependencias del dominio y de la aplicación sobre clases de producción; rechaza frameworks, otras capas y JDBC, y falla si no encuentra clases de dominio. |
| PMD 7.28.0 | Reglas de errores, buenas prácticas y rendimiento en producción y tests; reglas de diseño adicionales en producción. Prioridades 1–5. |
| SpotBugs 4.10.5 | Bytecode de producción y tests, esfuerzo máximo y umbral de confianza bajo. |
| CPD 7.28.0 | Bloques Java duplicados de al menos 100 tokens, ignorando anotaciones. |
| JaCoCo 0.8.15 | Mínimo del 85 % en líneas, ramas, métodos e instrucciones, sin excluir clases. |

Las instrucciones de JaCoCo son instrucciones de bytecode, no sentencias Java. Si no existen ramas, su cobertura no es una comprobación de decisiones de negocio. Un porcentaje alto en esta base mínima no demuestra cobertura del ejercicio completo.

Para exportar los informes sin instalar Maven:

```shell
docker build --target reports --output type=local,dest=target/quality .
```

Abre `target/quality/jacoco/index.html` para consultar cobertura. En la misma carpeta están los informes XML de los análisis y los resultados de las pruebas. `target/` está excluido de Git.

PMD se ejecuta en un proceso separado con el classpath del proyecto y del JDK para resolver los tipos. Las reglas están en `config/`. Se excluyen dos reglas de diseño obsoletas y `LoosePackageCoupling`, que requiere una política de paquetes; la independencia del dominio y de la aplicación ya se comprueba con ArchUnit; las reglas se ampliarán al introducir infraestructura. JaCoCo no tiene exclusiones. SpotBugs tiene una supresión local documentada en la preparación de mocks JDBC: esa configuración no abre recursos ni ejecuta SQL.

Mockito se carga como agente al iniciar la JVM de pruebas, conservando también el agente JaCoCo. La opción `-Xshare:off` se aplica solo a esas JVM porque la instrumentación de Mockito limita el uso de CDS. La imagen de ejecución no incorpora estos agentes ni esa opción.

La prueba `DomainArchitectureTest` usa el núcleo de [ArchUnit](https://www.archunit.org/userguide/html/000_Index.html) con JUnit, sin añadir otro motor de pruebas. ArchUnit tiene alcance `test` y no forma parte de la aplicación ejecutable. Las reglas admiten dependencias del dominio hacia Java y el propio dominio, y de aplicación hacia Java, dominio y aplicación, excepto JDBC; analiza bytecode y no certifica por sí sola que cada responsabilidad esté en la capa correcta.

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

## Oracle y pruebas de integración

Compose arranca Oracle Database Free 23.26.3.0 Lite con usuarios separados para aplicación y pruebas. Las contraseñas son de desarrollo local y Oracle no expone puertos al exterior. Cada usuario tiene una cuota de 256 MB en `SEARCH_DATA`, cuyo tamaño máximo es 1 GB.

```shell
docker compose --profile test build integration-test
docker compose up -d --wait oracle
docker compose --profile test run --rm integration-test
```

El primer arranque puede tardar varios minutos. Reconstruye el ejecutor cuando cambie el código. El perfil `integration` ejecuta las pruebas con Oracle real, además de las pruebas habituales y los controles de calidad. Los informes se guardan en `test-results/`, excluido de Git: `failsafe-reports/` para integración y `site/jacoco/` para cobertura.

Flyway 13.10.3 crea las tablas de solicitudes y outbox mediante `V1__initial_schema.sql`. Las pruebas comprueban la conexión, la migración repetible, las restricciones y el rollback conjunto. Flyway `clean` está desactivado. Las dependencias se descargan de Maven Central; se deshabilita el repositorio autenticado adicional declarado por Flyway.

Por ahora, la conexión y las migraciones se configuran desde las pruebas. La configuración del adaptador en Spring y el endpoint se añadirán en el siguiente paso.

Para detener Oracle conservando los datos:

```shell
docker compose stop oracle
```

El volumen `oracle-data` conserva el esquema entre arranques. Los cambios posteriores se añadirán en nuevas migraciones, sin modificar las ya aplicadas.

## Guardado de solicitudes

La solicitud y su evento pendiente se guardan juntos en Oracle. Si falla una escritura, se deshacen ambas. Los IDs repetidos no sobrescriben datos.

El evento puede ocupar hasta 262.144 bytes UTF-8. Si supera ese límite, se rechaza antes de guardar nada. Las pruebas cubren estos casos con Oracle real.
