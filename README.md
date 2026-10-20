# Búsquedas hoteleras

Servicio para registrar búsquedas hoteleras y consultar cuántas coinciden con sus criterios.

## Estado

Base generada con [Spring Initializr](https://start.spring.io/): Maven, Java 21 y Spring Web MVC. Incluye la clase principal y una prueba de carga del contexto. Todavía no hay endpoints de negocio ni conexiones a Oracle o Kafka.

## Construir y comprobar

Se necesita Docker con contenedores Linux y acceso a Internet en la primera construcción. No es necesario instalar Java ni Maven en el equipo.

```shell
docker build --progress=plain -t busquedas-hoteleras .
```

La construcción ejecuta `clean verify`, incluida la prueba de arranque y los controles de calidad, y genera la imagen ejecutable. La prueba inicia el contexto a través de `main` y comprueba que queda activo; todavía no verifica reglas de negocio.

## Controles de calidad

Los controles forman parte de `verify`: un incumplimiento hace fallar la construcción.

| Control | Alcance |
| --- | --- |
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

PMD se ejecuta en un proceso separado con el classpath del proyecto y del JDK para resolver los tipos. Las reglas están en `config/`. Se excluyen dos reglas de diseño obsoletas y `LoosePackageCoupling`, que requiere una política de paquetes; las restricciones hexagonales se incorporarán con sus pruebas cuando existan esas capas. SpotBugs y JaCoCo no tienen exclusiones.

Mockito se carga como agente al iniciar la JVM de pruebas, conservando también el agente JaCoCo. La opción `-Xshare:off` se aplica solo a esas JVM porque la instrumentación de Mockito limita el uso de CDS. La imagen de ejecución no incorpora estos agentes ni esa opción.

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
