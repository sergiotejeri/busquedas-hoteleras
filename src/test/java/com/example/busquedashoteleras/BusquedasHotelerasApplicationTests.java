package com.example.busquedashoteleras;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import javax.sql.DataSource;

@SpringBootTest(useMainMethod = SpringBootTest.UseMainMethod.ALWAYS, properties = {"spring.flyway.enabled=false", "app.outbox.enabled=false", "app.consumer.enabled=false"})
class BusquedasHotelerasApplicationTests {
    @MockitoBean
    private DataSource dataSource;

	@Test
	void startsApplicationContext(@Autowired ConfigurableApplicationContext context) {
		assertTrue(context.isActive(), "El contexto debe estar activo tras ejecutar main");
	}

}
