package com.samarait.connectorService;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/connectors")
public class ConnectorController {
	
	private final ConnectorService connectorService;
	
	public ConnectorController(ConnectorService connectorService) {
		this.connectorService = connectorService;
	}
	
	@PostMapping("/mysql-source")
	public String createMySQLSource() {
		
		return connectorService.createMySqlSourceConnector();
	}
	
	@PostMapping("/mysql-sink")
	public String createPostgresSink() {
		return connectorService.createPostgreSQLSinkConnector();
	}
	
	@GetMapping
	public String getConnector() {
		return connectorService.getConnectors();
	}
	
	@DeleteMapping("/{name}")
	public String deleteConnector(@PathVariable String name) {
		
		return connectorService.deleteConnector(name);
	}
	
	@GetMapping("/health")
	public String health() {
		return "Kafka Connect Demo is running!...";
	}

}
