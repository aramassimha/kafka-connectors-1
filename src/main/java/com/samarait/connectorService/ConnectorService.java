package com.samarait.connectorService;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ConnectorService {
    
    // ✅ FIXED: Use Docker service name instead of localhost
    private final RestTemplate restTemplate;
    private final String KAFKA_CONNECT_URL = "http://kafka-connect:8083/connectors";
    
    
    public ConnectorService() {
        this.restTemplate = new RestTemplate();
    }
    
    public String createMySqlSourceConnector() {
        
        // ✅ FIXED: Corrected JSON syntax and class names
        String connectorConfig = """
                {
                    "name": "mysql-source-connector",
                    "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                        "connection.url": "jdbc:mysql://mysql:3306/source_db",
                        "connection.user": "root",
                        "connection.password": "password",
                        "table.whitelist": "users",
                        "mode": "incrementing",
                        "incrementing.column.name": "id",
                        "topic.prefix": "mysql-",
                        "poll.interval.ms": "5000",
                        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "key.converter.schemas.enable": "false",
                        "value.converter.schemas.enable": "false"
                    }
                }
                """;
        return createConnector(connectorConfig);
    }
    
    public String createPostgreSQLSinkConnector() {
        
        // ✅ FIXED: Corrected all issues in PostgreSQL config
        String connectorConfig = """
                {
                    "name": "postgres-sink-connector",
                    "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                        "connection.url": "jdbc:postgresql://postgres:5432/sink_db",
                        "connection.user": "postgres",
                        "connection.password": "password",
                        "topics": "mysql-users",
                        "table.name.format": "users",
                        "insert.mode": "upsert",
                        "pk.mode": "record_value",
                        "pk.fields": "id",
                        "auto.create": "true",
                        "auto.evolve": "true",
                        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "key.converter.schemas.enable": "false",
                        "value.converter.schemas.enable": "false"
                    }
                }
                """;
        return createConnector(connectorConfig);
    }
    
    private String createConnector(String configJson) {
        
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<>(configJson, headers);
            
            ResponseEntity<String> response = restTemplate.postForEntity(
                    KAFKA_CONNECT_URL, request, String.class);
            return "Connector created: " + response.getBody();
            
        }
        catch(Exception ex) {
            return "Error: " + ex.getMessage();
        }
    }                                                                                                                           
    
    public String getConnectors() {
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(KAFKA_CONNECT_URL, String.class);
            return "Active connectors: " + response.getBody();
        }
        catch(Exception ex) {
            return "Error: " + ex.getMessage();
        }
    }
    
    public String deleteConnector(String connectName) {
        
        try {
            restTemplate.delete(KAFKA_CONNECT_URL + "/" + connectName);
            return "Deleted: " + connectName;
        }
        catch(Exception ex) {
            return "Error: " + ex.getMessage();
        }
    }
}
