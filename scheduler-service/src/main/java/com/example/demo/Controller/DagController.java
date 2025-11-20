package com.example.demo.Controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.example.demo.service.OrchestratorService;
import com.example.demo.dto.DagStatusResponse;
import com.example.demo.dto.SystemStatusResponse; // NEW Import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1") // Base path changed to /api/v1
@CrossOrigin(origins = {"http://localhost:3000", "http://localhost"})
public class DagController {

    private static final Logger LOGGER = LoggerFactory.getLogger(DagController.class);
    private final OrchestratorService orchestratorService;

    @Autowired
    public DagController(OrchestratorService orchestratorService) {
        this.orchestratorService = orchestratorService;
    }

    // --- NEW ENDPOINT for System Health ---
    @GetMapping("/system/status")
    public ResponseEntity<SystemStatusResponse> getSystemStatus() {
        try {
            SystemStatusResponse status = orchestratorService.getSystemStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            LOGGER.error("Failed to get system status", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    // --- End of new endpoint ---

    @GetMapping("/dags/{dagId}")
    public ResponseEntity<Object> getDagStatus(@PathVariable String dagId) {
        DagStatusResponse statusResponse = orchestratorService.getDagStatus(dagId);
        if (statusResponse != null) {
            return ResponseEntity.ok(statusResponse);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/dags")
    public ResponseEntity<Object> submitDag(@RequestBody JsonNode dagPayload) {
        LOGGER.info("Received a new DAG submission via API endpoint.");
        String dagId = orchestratorService.processNewDagSubmission(dagPayload);

        if (dagId != null) {
            Map<String, String> response = Map.of(
                    "message", "DAG submitted and is being processed.",
                    "dagId", dagId,
                    "status", "PENDING"
            );
            return ResponseEntity.accepted().body(response);
        } else {
            Map<String, String> response = Map.of(
                    "message", "Failed to process DAG submission."
            );
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @GetMapping("/dags/{dagName}/metrics")
    public ResponseEntity<Object> getDagMetrics(
            @PathVariable String dagName,
            @RequestParam(defaultValue = "20") int limit) {
        try {
            var metrics = orchestratorService.getDagRunMetrics(dagName, limit);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            LOGGER.error("Failed to get metrics for DAG: {}", dagName, e);
            return ResponseEntity.internalServerError().build();
        }
    }
}