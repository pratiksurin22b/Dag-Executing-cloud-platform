import React, { useState, useRef, useEffect } from 'react';
import jsyaml from 'js-yaml';
import { Edit3, CheckCircle, XCircle, Loader, Clock, GitBranch } from 'lucide-react';

// A more complex, self-contained DAG to better showcase the UI's capabilities
const sampleYaml = `apiVersion: v1
dagName: "complex-system-readiness-check"
tasks:
  - name: "initialize-system"
    type: "command"
    image: "ubuntu:latest"
    command: ["/bin/bash", "-c", "echo 'System initialization started...' && sleep 2 && echo 'Initialization complete.'"]
  - name: "check-database-connection"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["initialize-system"]
    command: ["/bin/bash", "-c", "echo 'Pinging database...' && sleep 4 && echo 'Database connection OK.'"]
  - name: "check-storage-access"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["initialize-system"]
    command: ["/bin/bash", "-c", "echo 'Verifying storage...' && sleep 2 && echo 'Storage access OK.'"]
  - name: "run-system-diagnostics"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["check-database-connection", "check-storage-access"]
    command: ["/bin/bash", "-c", "echo 'Running full system diagnostics...' && sleep 5 && echo 'All systems nominal.'"]
  - name: "report-system-ready"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["run-system-diagnostics"]
    command: ["/bin/bash", "-c", "echo 'SUCCESS: System is fully operational and ready.'"]
`;

const apiUrl = '/api/v1/dags'; // This will be correctly proxied by Nginx in Docker

// A small, reusable component to display a status icon based on the task's state.
const StatusIcon = ({ status }) => {
    switch (status) {
        case 'SUCCEEDED': return <CheckCircle size={20} color="#00ff8c" data-testid="status-succeeded" />;
        case 'FAILED': return <XCircle size={20} color="#ff4d4d" data-testid="status-failed" />;
        case 'QUEUED': return <Loader size={20} color="#00f5ff" className="animate-spin" data-testid="status-queued" />;
        case 'PENDING': return <Clock size={20} color="#94a3b8" data-testid="status-pending" />;
        default: return <Clock size={20} color="#94a3b8" />;
    }
};

// A component to render a single task's status, dependencies, and logs.
const TaskItem = ({ task }) => (
    <div style={styles.taskItem}>
        <div style={styles.taskHeader}>
            <StatusIcon status={task.status} />
            <span style={styles.taskName}>{task.name}</span>
            <span style={styles.taskStatus}>{task.status || 'PENDING'}</span>
        </div>
        {task.dependsOn && task.dependsOn.length > 0 && (
            <div style={styles.dependencies}>
                <GitBranch size={14} color="#94a3b8" />
                <span>Depends on: {task.dependsOn.join(', ')}</span>
            </div>
        )}
        {task.logs && task.logs.length > 0 && (
            <div style={styles.logs}>
                {task.logs.map((log, index) => <div key={index}>> {log}</div>)}
            </div>
        )}
    </div>
);

// The main component, now acting as a full dashboard.
const DagMonitor = () => {
    const [yamlText, setYamlText] = useState(sampleYaml);
    const [dagState, setDagState] = useState(null); // Will hold the entire state of the DAG run
    const [error, setError] = useState('');
    const [isSubmitting, setIsSubmitting] = useState(false);
    const pollingIntervalRef = useRef(null);

    // This is the core of the real-time functionality.
    // It starts polling for updates after a DAG is successfully submitted.
    useEffect(() => {
        // Stop any previous polling if it exists
        if (pollingIntervalRef.current) {
            clearInterval(pollingIntervalRef.current);
        }

        if (dagState?.dagId) {
            pollingIntervalRef.current = setInterval(async () => {
                try {
                    const response = await fetch(`${apiUrl}/${dagState.dagId}`);
                    if (response.ok) {
                        const data = await response.json();
                        setDagState(data); // Update the UI with the new state from the backend
                    } else {
                        // Stop polling if the DAG is not found (e.g., expired from Redis)
                        clearInterval(pollingIntervalRef.current);
                    }
                } catch (e) {
                    console.error("Polling error:", e);
                    clearInterval(pollingIntervalRef.current);
                }
            }, 2000); // Poll for new status every 2 seconds
        }

        // Cleanup function: This is called when the component is unmounted
        // to prevent memory leaks from the interval.
        return () => {
            if (pollingIntervalRef.current) {
                clearInterval(pollingIntervalRef.current);
            }
        };
    }, [dagState?.dagId]);


    const handleSubmit = async () => {
        setIsSubmitting(true);
        setError('');
        setDagState(null); // Clear any previous DAG run from the display

        try {
            const dagObject = jsyaml.load(yamlText);
            const apiResponse = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(dagObject),
            });
            if (!apiResponse.ok) throw new Error(`Server Error: ${apiResponse.status}`);
            const result = await apiResponse.json();

            // Set the initial state of the dashboard to kick off the polling.
            const initialTasks = dagObject.tasks.map(t => ({
                name: t.name,
                status: 'PENDING',
                dependsOn: t.depends_on || [],
                logs: []
            }));
            setDagState({ dagId: result.dagId, dagName: dagObject.dagName, tasks: initialTasks });

        } catch (err) {
            setError(err.message);
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <div style={styles.container}>
            <div style={styles.panel}>
                <h1 style={styles.title}>DAG Orchestration Terminal</h1>
                <div style={styles.editorContainer}>
                    <textarea style={styles.textArea} value={yamlText} onChange={(e) => setYamlText(e.target.value)} />
                </div>
                <button style={styles.submitButton} onClick={handleSubmit} disabled={isSubmitting}>
                    {isSubmitting ? 'Submitting...' : 'Initiate Workflow'}
                </button>
                {error && <div style={styles.errorBox}>{error}</div>}
            </div>

            {/* This entire section is new. It only renders after a DAG has been submitted. */}
            {dagState && (
                 <div style={{...styles.panel, marginTop: '20px'}}>
                     <h2 style={styles.title}>Live DAG Run: {dagState.dagName}</h2>
                     <p style={styles.dagId}>DAG Run ID: {dagState.dagId}</p>
                     <div style={styles.taskList}>
                         {dagState.tasks.map(task => <TaskItem key={task.name} task={task} />)}
                     </div>
                 </div>
            )}
        </div>
    );
};

// Updated styles for the new dashboard layout
const styles = {
    container: { width: '90%', margin: '0 auto', fontFamily: 'Arial, sans-serif', padding: '20px 0' },
    panel: { width: '100%', maxWidth: '900px', backgroundColor: '#101827', border: '1px solid #00f5ff', borderRadius: '8px', padding: '30px', boxShadow: '0 0 25px rgba(0, 245, 255, 0.3)', margin: '0 auto' },
    title: { color: '#00f5ff', textAlign: 'center', margin: '0 0 20px 0' },
    editorContainer: { marginBottom: '20px' },
    textArea: { width: '100%', height: '350px', backgroundColor: '#0a0f18', border: '1px solid #334155', color: '#e0e0e0', borderRadius: '4px', padding: '15px', fontFamily: 'monospace', fontSize: '14px', boxSizing: 'border-box' },
    submitButton: { width: '100%', background: '#00f5ff', border: 'none', color: '#0a0f18', padding: '15px', borderRadius: '4px', fontSize: '18px', fontWeight: 'bold', cursor: 'pointer', transition: 'opacity 0.2s' },
    errorBox: { marginTop: '15px', padding: '10px', backgroundColor: '#ff4d4d20', border: '1px solid #ff4d4d', color: 'white', borderRadius: '4px', fontFamily: 'monospace' },
    dagId: { color: '#94a3b8', textAlign: 'center', marginBottom: '20px', wordBreak: 'break-all', fontSize: '14px' },
    taskList: { display: 'flex', flexDirection: 'column', gap: '10px' },
    taskItem: { backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '4px', padding: '15px', transition: 'all 0.3s' },
    taskHeader: { display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' },
    taskName: { color: 'white', fontWeight: 'bold', flexGrow: 1 },
    taskStatus: { fontStyle: 'italic', color: '#e0e0e0' },
    dependencies: { display: 'flex', alignItems: 'center', gap: '8px', color: '#94a3b8', fontSize: '12px', marginBottom: '10px', paddingLeft: '5px' },
    logs: { backgroundColor: '#0a0f18', padding: '10px', borderRadius: '4px', fontFamily: 'monospace', fontSize: '12px', color: '#e0e0e0', whiteSpace: 'pre-wrap', maxHeight: '150px', overflowY: 'auto', border: '1px solid #334155' },
};

export default DagMonitor;