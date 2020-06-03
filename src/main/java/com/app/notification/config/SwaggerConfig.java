package com.app.notification.config;

import java.util.Arrays;
import java.util.HashSet;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

	// Swagger Metadata: http://localhost:8083/v2/api-docs
	// Swagger UI URL: http://localhost:8083/swagger-ui.html

	@Bean
	public Docket api()
	{
		return new Docket(DocumentationType.SWAGGER_2)
				.apiInfo(getApiInfo())
				.produces(new HashSet<>(Arrays.asList("application/xml", "application/json")))
				.select()
				.apis(RequestHandlerSelectors.basePackage("com.app.notification"))
				.build();
	}

	private ApiInfo getApiInfo() {

		return new ApiInfoBuilder()
				.title("Nineleaps Notification Management Service")
				.description("This page lists all API's of Notification Service")
				.version("2.0")
				.contact(new Contact("Thejaswi Kumar ", "https://nineleaps.com", "tj@nineleaps.com"))
				.license("License 2.0")
				.licenseUrl("https://nineleaps.com/license.html")
				.build();
	}
}