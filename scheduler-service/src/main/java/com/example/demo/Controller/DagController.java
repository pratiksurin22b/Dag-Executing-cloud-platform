package com.example.demo.Controller;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dags")
public class DagController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DagController.class);

    @PostMapping
    public ResponseEntity<Object> submitDag(@RequestBody JsonNode dagPayload) {
        // For this phase, we simply log that we received the DAG.
        // The JsonNode type is flexible and accepts any valid JSON.
        LOGGER.info("Received a new DAG submission.");
        LOGGER.info("Payload: {}", dagPayload.toString());

        // In the future, this will trigger the Orchestrator.
        // For now, we return a hardcoded success response.
        String newDagId = "dag-" + UUID.randomUUID().toString();

        Map<String, String> response = Map.of(
                "message", "DAG submitted successfully",
                "dagId", newDagId,
                "status", "PENDING"
        );

        return ResponseEntity.accepted().body(response);
    }
}

