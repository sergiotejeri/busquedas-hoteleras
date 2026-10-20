package com.example.busquedashoteleras;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootTest(useMainMethod = SpringBootTest.UseMainMethod.ALWAYS)
class BusquedasHotelerasApplicationTests {

	@Test
	void startsApplicationContext(@Autowired ConfigurableApplicationContext context) {
		assertTrue(context.isActive(), "El contexto debe estar activo tras ejecutar main");
	}

}
