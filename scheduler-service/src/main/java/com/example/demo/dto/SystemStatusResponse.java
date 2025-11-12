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
        private String status; // Healthy, Starting, Degraded, CrashLoop, ImagePullError, NotReady, Stopped, ScaledDown, Unresponsive
        private int runningReplicas;
        private int desiredReplicas;
        private String podName; // Example: name of the first running pod

        // New, richer breakdown
        private int readyReplicas;
        private int notReadyReplicas;
        private int pendingReplicas;
        private int crashLoopingReplicas;
        private int imagePullBackOffReplicas;
        private List<PodBrief> pods; // per-pod summary

        public ServiceStatus(String name,
                             String status,
                             int runningReplicas,
                             int desiredReplicas,
                             String podName,
                             int readyReplicas,
                             int notReadyReplicas,
                             int pendingReplicas,
                             int crashLoopingReplicas,
                             int imagePullBackOffReplicas,
                             List<PodBrief> pods) {
            this.name = name;
            this.status = status;
            this.runningReplicas = runningReplicas;
            this.desiredReplicas = desiredReplicas;
            this.podName = podName;
            this.readyReplicas = readyReplicas;
            this.notReadyReplicas = notReadyReplicas;
            this.pendingReplicas = pendingReplicas;
            this.crashLoopingReplicas = crashLoopingReplicas;
            this.imagePullBackOffReplicas = imagePullBackOffReplicas;
            this.pods = pods;
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
        public int getReadyReplicas() { return readyReplicas; }
        public void setReadyReplicas(int readyReplicas) { this.readyReplicas = readyReplicas; }
        public int getNotReadyReplicas() { return notReadyReplicas; }
        public void setNotReadyReplicas(int notReadyReplicas) { this.notReadyReplicas = notReadyReplicas; }
        public int getPendingReplicas() { return pendingReplicas; }
        public void setPendingReplicas(int pendingReplicas) { this.pendingReplicas = pendingReplicas; }
        public int getCrashLoopingReplicas() { return crashLoopingReplicas; }
        public void setCrashLoopingReplicas(int crashLoopingReplicas) { this.crashLoopingReplicas = crashLoopingReplicas; }
        public int getImagePullBackOffReplicas() { return imagePullBackOffReplicas; }
        public void setImagePullBackOffReplicas(int imagePullBackOffReplicas) { this.imagePullBackOffReplicas = imagePullBackOffReplicas; }
        public List<PodBrief> getPods() { return pods; }
        public void setPods(List<PodBrief> pods) { this.pods = pods; }
    }

    /**
     * Per-pod summary for quick diagnosis in the UI.
     */
    public static class PodBrief {
        private String name;
        private String phase; // Pending/Running/Succeeded/Failed/Unknown
        private boolean ready;
        private int restarts;
        private String reason; // e.g., CrashLoopBackOff, ImagePullBackOff

        // NEW: list of pod condition types that are True
        private List<String> trueConditions;
        // NEW: list of container level waiting reasons (for multi-container pods)
        private List<String> containerWaitingReasons;

        public PodBrief(String name, String phase, boolean ready, int restarts, String reason, List<String> trueConditions, List<String> containerWaitingReasons) {
            this.name = name;
            this.phase = phase;
            this.ready = ready;
            this.restarts = restarts;
            this.reason = reason;
            this.trueConditions = trueConditions;
            this.containerWaitingReasons = containerWaitingReasons;
        }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getPhase() { return phase; }
        public void setPhase(String phase) { this.phase = phase; }
        public boolean isReady() { return ready; }
        public void setReady(boolean ready) { this.ready = ready; }
        public int getRestarts() { return restarts; }
        public void setRestarts(int restarts) { this.restarts = restarts; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
        public List<String> getTrueConditions() { return trueConditions; }
        public void setTrueConditions(List<String> c) { this.trueConditions = c; }
        public List<String> getContainerWaitingReasons() { return containerWaitingReasons; }
        public void setContainerWaitingReasons(List<String> r) { this.containerWaitingReasons = r; }
    }
}