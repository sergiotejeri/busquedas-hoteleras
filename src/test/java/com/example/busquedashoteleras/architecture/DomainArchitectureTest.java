package com.example.busquedashoteleras.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

class DomainArchitectureTest {
    @Test
    void domainAndApplicationRespectDependencyDirection() {
        // ArchRule.check requiere JavaClasses; no acepta una interfaz de coleccion.
        @SuppressWarnings("PMD.LooseCoupling")
        JavaClasses productionClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.example.busquedashoteleras");

        ArchRule rule = classes()
                .that().resideInAPackage("com.example.busquedashoteleras.domain..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage("java..", "com.example.busquedashoteleras.domain..")
                .andShould().onlyDependOnClassesThat()
                .resideOutsideOfPackages("java.sql..", "javax.sql..")
                .because("el dominio debe ser independiente de frameworks y adaptadores")
                .allowEmptyShould(false);

        ArchRule applicationRule = classes()
                .that().resideInAPackage("com.example.busquedashoteleras.application..")
                .should().onlyDependOnClassesThat()
                .resideInAnyPackage("java..", "com.example.busquedashoteleras.domain..",
                        "com.example.busquedashoteleras.application..")
                .andShould().onlyDependOnClassesThat()
                .resideOutsideOfPackages("java.sql..", "javax.sql..")
                .because("la aplicacion coordina el dominio sin conocer infraestructura")
                .allowEmptyShould(false);

        assertAll(
                () -> assertDoesNotThrow(() -> rule.check(productionClasses), "Dominio independiente"),
                () -> assertDoesNotThrow(() -> applicationRule.check(productionClasses),
                        "Aplicacion independiente de infraestructura"));
    }
}
