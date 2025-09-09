package com.example.demo.dto;

import java.util.List;
import java.util.Map;

// This class defines the structure of the JSON object we will send to the frontend.
public class DagStatusResponse {

    private String dagId;
    private String dagName;
    private List<TaskStatus> tasks;

    public DagStatusResponse(String dagId, String dagName, List<TaskStatus> tasks) {
        this.dagId = dagId;
        this.dagName = dagName;
        this.tasks = tasks;
    }

    // Getters and Setters
    public String getDagId() { return dagId; }
    public void setDagId(String dagId) { this.dagId = dagId; }
    public String getDagName() { return dagName; }
    public void setDagName(String dagName) { this.dagName = dagName; }
    public List<TaskStatus> getTasks() { return tasks; }
    public void setTasks(List<TaskStatus> tasks) { this.tasks = tasks; }


    // A nested static class to represent the state of a single task.
    public static class TaskStatus {
        private String name;
        private String status;
        private List<String> dependsOn;
        private List<String> logs;

        public TaskStatus(String name, String status, List<String> dependsOn, List<String> logs) {
            this.name = name;
            this.status = status;
            this.dependsOn = dependsOn;
            this.logs = logs;
        }

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public List<String> getDependsOn() { return dependsOn; }
        public void setDependsOn(List<String> dependsOn) { this.dependsOn = dependsOn; }
        public List<String> getLogs() { return logs; }
        public void setLogs(List<String> logs) { this.logs = logs; }
    }
}