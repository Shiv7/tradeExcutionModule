package com.kotsin.execution.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.ExternalDocumentation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * OpenAPI/Swagger configuration for the Trade Execution Module
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Kotsin Trade Execution API")
                        .description("Real-time trade execution engine that processes strategy signals, manages active trades, and provides comprehensive monitoring capabilities. " +
                                    "This module handles signal consumption from multiple strategy modules, trade lifecycle management, risk management, and P&L tracking.")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Kotsin Development Team")
                                .email("dev@kotsin.com")
                                .url("https://kotsin.com"))
                        .license(new License()
                                .name("Private License")
                                .url("https://kotsin.com/license")))
                .servers(List.of(
                        new Server()
                                .url("/")
                                .description("Relative base URL (adapts to active environment)")))
                .externalDocs(new ExternalDocumentation()
                        .description("Kotsin Trading Documentation")
                        .url("https://docs.kotsin.com"));
    }
}
