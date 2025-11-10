package com.example.demo.dto;

import java.util.List;

/**
 * DTO to hold the real-time status of all core platform services.
 */
public class SystemStatusResponse {

    private List<ServiceStatus> services;

    public SystemStatusResponse(List<ServiceStatus> services) {
        this.services = services;
    }

    public List<ServiceStatus> getServices() {
        return services;
    }

    public void setServices(List<ServiceStatus> services) {
        this.services = services;
    }

    /**
     * Nested class to represent the status of a single service.
     */
    public static class ServiceStatus {
        private String name;
        private String status;
        private int runningReplicas;
        private int desiredReplicas;
        private String podName; // Example: name of the first running pod

        public ServiceStatus(String name, String status, int runningReplicas, int desiredReplicas, String podName) {
            this.name = name;
            this.status = status;
            this.runningReplicas = runningReplicas;
            this.desiredReplicas = desiredReplicas;
            this.podName = podName;
        }

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public int getRunningReplicas() { return runningReplicas; }
        public void setRunningReplicas(int runningReplicas) { this.runningReplicas = runningReplicas; }
        public int getDesiredReplicas() { return desiredReplicas; }
        public void setDesiredReplicas(int desiredReplicas) { this.desiredReplicas = desiredReplicas; }
        public String getPodName() { return podName; }
        public void setPodName(String podName) { this.podName = podName; }
    }
}