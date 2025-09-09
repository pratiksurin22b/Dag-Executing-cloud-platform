package com.example.demo.Controller;
import com.fasterxml.jackson.databind.JsonNode;
import com.example.demo.service.OrchestratorService;
import com.example.demo.dto.DagStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/dags")
// We keep the CORS config here for easy development and Docker access
@CrossOrigin(origins = {"http://localhost:3000", "http://localhost"})
public class DagController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DagController.class);

    // 1. We declare a dependency on our new OrchestratorService.
    private final OrchestratorService orchestratorService;

    // 2. We use constructor injection to get an instance of the service from Spring.
    // This is a best practice for dependency injection.
    @Autowired
    public DagController(OrchestratorService orchestratorService) {
        this.orchestratorService = orchestratorService;
    }

    @GetMapping("/{dagId}")
    public ResponseEntity<Object> getDagStatus(@PathVariable String dagId) {
        DagStatusResponse statusResponse = orchestratorService.getDagStatus(dagId);
        if (statusResponse != null) {
            return ResponseEntity.ok(statusResponse);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping
    public ResponseEntity<Object> submitDag(@RequestBody JsonNode dagPayload) {
        LOGGER.info("Received a new DAG submission via API endpoint.");

        // 3. Instead of just logging, we now call the orchestrator to do the real work.
        // The service will handle saving to Redis and dispatching to RabbitMQ.
        String dagId = orchestratorService.processNewDagSubmission(dagPayload);

        // 4. We check the result from the service to provide a meaningful response.
        if (dagId != null) {
            // If the service returns a DAG ID, the submission was successful.
            Map<String, String> response = Map.of(
                    "message", "DAG submitted and is being processed.",
                    "dagId", dagId,
                    "status", "PENDING" // The initial overall status of the DAG.
            );
            return ResponseEntity.accepted().body(response);
        } else {
            // If the service returns null, something went wrong.
            Map<String, String> response = Map.of(
                    "message", "Failed to process DAG submission."
            );
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
