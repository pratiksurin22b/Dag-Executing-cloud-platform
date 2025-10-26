import React, { useState, useRef, useEffect } from 'react';
import jsyaml from 'js-yaml';
// Import the new icon for the skipped/failed upstream status
import { Edit3, CheckCircle, XCircle, Loader, Clock, GitBranch, AlertTriangle } from 'lucide-react';

const sampleYaml = `apiVersion: v1
dagName: "guaranteed-failure-test"
tasks:
  - name: "start-process"
    type: "command"
    image: "ubuntu:latest"
    command: ["/bin/bash", "-c", "echo 'Process startup sequence initiated.'"]
  - name: "check-critical-resource"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["start-process"]
    command: ["/bin/bash", "-c", "echo 'Checking for /etc/required_config...' && ls /etc/required_config"]
  - name: "run-main-application"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["check-critical-resource"]
    command: ["/bin/bash", "-c", "echo 'CRITICAL ERROR: This command should never run.'"]
`;

const apiUrl = '/api/v1/dags';

// UPGRADED StatusIcon component
const StatusIcon = ({ status }) => {
    switch (status) {
        case 'SUCCEEDED': return <CheckCircle size={20} color="#00ff8c" />;
        case 'FAILED': return <XCircle size={20} color="#ff4d4d" />;
        case 'QUEUED': return <Loader size={20} color="#00f5ff" className="animate-spin" />;
        case 'PENDING': return <Clock size={20} color="#94a3b8" />;
        // NEW CASE: for tasks that will never run
        case 'UPSTREAM_FAILED': return <AlertTriangle size={20} color="#f59e0b" />;
        default: return <Clock size={20} color="#94a3b8" />;
    }
};

// ... The rest of the DagSubmitter.jsx and TaskItem components remain exactly the same as before ...
// (For brevity, only the changed parts are shown, but you should replace the entire file
// to ensure all the latest code is included.)

const TaskItem = ({ task }) => (
    <div style={styles.taskItem}>
        <div style={styles.taskHeader}>
            <StatusIcon status={task.status} />
            <span style={styles.taskName}>{task.name}</span>
            <span style={{...styles.taskStatus, ...styles.statusColors[task.status]}}>{task.status || 'PENDING'}</span>
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

const DagMonitor = () => {
    // All state hooks and functions (useEffect, handleSubmit) are the same as before.
    const [yamlText, setYamlText] = useState(sampleYaml);
    const [dagState, setDagState] = useState(null);
    const [error, setError] = useState('');
    const [isSubmitting, setIsSubmitting] = useState(false);
    const pollingIntervalRef = useRef(null);

    useEffect(() => {
        if (pollingIntervalRef.current) {
            clearInterval(pollingIntervalRef.current);
        }
        if (dagState?.dagId) {
            pollingIntervalRef.current = setInterval(async () => {
                try {
                    const response = await fetch(`${apiUrl}/${dagState.dagId}`);
                    if (response.ok) {
                        const data = await response.json();
                        setDagState(data);
                    } else {
                        clearInterval(pollingIntervalRef.current);
                    }
                } catch (e) {
                    console.error("Polling error:", e);
                    clearInterval(pollingIntervalRef.current);
                }
            }, 2000);
        }
        return () => {
            if (pollingIntervalRef.current) {
                clearInterval(pollingIntervalRef.current);
            }
        };
    }, [dagState?.dagId]);

    const handleSubmit = async () => {
        setIsSubmitting(true);
        setError('');
        setDagState(null);
        try {
            const dagObject = jsyaml.load(yamlText);
            const apiResponse = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(dagObject),
            });
            if (!apiResponse.ok) throw new Error(`Server Error: ${apiResponse.status}`);
            const result = await apiResponse.json();
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

// Updated styles to add specific colors for the status text
const styles = {
    // ... all other styles are the same
    container: { width: '90%', margin: '0 auto', fontFamily: 'Arial, sans-serif', padding: '20px 0' },
    panel: { width: '100%', maxWidth: '900px', backgroundColor: '#101827', border: '1px solid #00f5ff', borderRadius: '8px', padding: '30px', boxShadow: '0 0 25px rgba(0, 245, 255, 0.3)', margin: '0 auto' },
    title: { color: '#00f5ff', textAlign: 'center', margin: '0 0 20px 0' },
    editorContainer: { marginBottom: '20px' },
    textArea: { width: '100%', height: '250px', backgroundColor: '#0a0f18', border: '1px solid #334155', color: '#e0e0e0', borderRadius: '4px', padding: '15px', fontFamily: 'monospace', fontSize: '14px', boxSizing: 'border-box' },
    submitButton: { width: '100%', background: '#00f5ff', border: 'none', color: '#0a0f18', padding: '15px', borderRadius: '4px', fontSize: '18px', fontWeight: 'bold', cursor: 'pointer', transition: 'opacity 0.2s' },
    errorBox: { marginTop: '15px', padding: '10px', backgroundColor: '#ff4d4d20', border: '1px solid #ff4d4d', color: 'white', borderRadius: '4px', fontFamily: 'monospace' },
    dagId: { color: '#94a3b8', textAlign: 'center', marginBottom: '20px', wordBreak: 'break-all', fontSize: '14px' },
    taskList: { display: 'flex', flexDirection: 'column', gap: '10px' },
    taskItem: { backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '4px', padding: '15px', transition: 'all 0.3s' },
    taskHeader: { display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '10px' },
    taskName: { color: 'white', fontWeight: 'bold', flexGrow: 1 },
    taskStatus: { fontStyle: 'italic', fontWeight: 'bold' },
    statusColors: {
        SUCCEEDED: { color: '#00ff8c' },
        FAILED: { color: '#ff4d4d' },
        QUEUED: { color: '#00f5ff' },
        PENDING: { color: '#94a3b8' },
        UPSTREAM_FAILED: { color: '#f59e0b' },
    },
    dependencies: { display: 'flex', alignItems: 'center', gap: '8px', color: '#94a3b8', fontSize: '12px', marginBottom: '10px', paddingLeft: '5px' },
    logs: { backgroundColor: '#0a0f18', padding: '10px', borderRadius: '4px', fontFamily: 'monospace', fontSize: '12px', color: '#e0e0e0', whiteSpace: 'pre-wrap', maxHeight: '150px', overflowY: 'auto', border: '1px solid #334155' },
};

export default DagMonitor;