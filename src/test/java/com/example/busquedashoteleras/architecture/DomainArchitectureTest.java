package com.example.busquedashoteleras.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

class DomainArchitectureTest {
    @Test
    void domainDependsOnlyOnJavaAndItself() {
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

        assertDoesNotThrow(() -> rule.check(productionClasses),
                "Las dependencias del dominio deben respetar los limites de arquitectura");
    }
}
