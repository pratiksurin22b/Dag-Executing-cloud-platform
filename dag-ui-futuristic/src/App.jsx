import React, { useState, useRef, useEffect } from 'react';
import jsyaml from 'js-yaml';
import { 
    Edit3, CheckCircle, XCircle, Loader, Clock, GitBranch, AlertTriangle, 
    Terminal, Monitor, Database, Rabbit, Box, Server, CloudOff 
} from 'lucide-react';

// --- Page 1: DAG Monitor (Your existing component, renamed) ---
const DagMonitor = () => {
    const [yamlText, setYamlText] = useState(sampleYaml);
    const [dagState, setDagState] = useState(null);
    const [error, setError] = useState('');
    const [isSubmitting, setIsSubmitting] = useState(false);
    const pollingIntervalRef = useRef(null);
    const apiUrl = '/api/v1/dags';

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
                        // Stop polling if DAG is not found (e.g., cleared from Redis)
                        clearInterval(pollingIntervalRef.current);
                    }
                } catch (e) {
                    console.error("Polling error:", e);
                    clearInterval(pollingIntervalRef.current);
                }
            }, 2000); // Poll every 2 seconds
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
        if (pollingIntervalRef.current) {
            clearInterval(pollingIntervalRef.current);
        }
        
        try {
            const dagObject = jsyaml.load(yamlText);
            const apiResponse = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(dagObject),
            });
            if (!apiResponse.ok) throw new Error(`Server Error: ${apiResponse.status} ${apiResponse.statusText}`);
            const result = await apiResponse.json();
            
            // Set initial state based on submission
            const initialTasks = dagObject.tasks.map(t => ({
                name: t.name,
                status: 'PENDING',
                dependsOn: t.depends_on || [],
                logs: []
            }));
            setDagState({ dagId: result.dagId, dagName: dagObject.dagName, tasks: initialTasks });
        } catch (err) {
            setError(err.message || 'Failed to submit DAG. Check YAML format.');
        } finally {
            setIsSubmitting(false);
        }
    };

    return (
        <div style={styles.container}>
            <div style={styles.panel}>
                <h1 style={styles.title}><Terminal size={28} style={{ marginRight: '10px' }}/>DAG Orchestration Terminal</h1>
                <div style={styles.editorContainer}>
                    <textarea style={styles.textArea} value={yamlText} onChange={(e) => setYamlText(e.target.value)} />
                </div>
                <button style={styles.submitButton} onClick={handleSubmit} disabled={isSubmitting}>
                    {isSubmitting ? <Loader className="animate-spin" /> : <Edit3 size={18} />}
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

const StatusIcon = ({ status }) => {
    switch (status) {
        case 'SUCCEEDED': return <CheckCircle size={20} color="#00ff8c" />;
        case 'FAILED': return <XCircle size={20} color="#ff4d4d" />;
        case 'K8S_JOB_SUBMITTED':
        case 'K8S_JOB_CREATING':
        case 'QUEUED': return <Loader size={20} color="#00f5ff" className="animate-spin" />;
        case 'PENDING': return <Clock size={20} color="#94a3b8" />;
        case 'UPSTREAM_FAILED': 
        case 'DISPATCH_FAILED': return <AlertTriangle size={20} color="#f59e0b" />;
        default: return <Clock size={20} color="#94a3b8" />;
    }
};

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
                {/* --- THIS IS THE FIX --- */}
                {/* Changed `>> {log}` to `'> ' + log` to avoid JSX parsing error */}
                {task.logs.map((log, index) => <div key={index}>{'> ' + log}</div>)}
                {/* --- END OF FIX --- */}
            </div>
        )}
    </div>
);

const sampleYaml = `apiVersion: v1
dagName: "k8s-sequential-test"
tasks:
  - name: "start-job"
    type: "command"
    image: "ubuntu:latest"
    command: ["/bin/bash", "-c", "echo 'K8s Task 1: Starting...' && sleep 2"]
  - name: "process-step"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["start-job"]
    command: ["/bin/bash", "-c", "echo 'K8s Task 2: Processing...' && sleep 3"]
  - name: "finish-job"
    type: "command"
    image: "ubuntu:latest"
    depends_on: ["process-step"]
    command: ["/bin/bash", "-c", "echo 'K8s Task 3: Job finished.'"]
`;
// --- End of Page 1 ---


// --- Page 2: System Health Monitor (NEW) ---
const SystemMonitor = () => {
    const [systemStatus, setSystemStatus] = useState([]);
    const [error, setError] = useState(null);
    const apiUrl = '/api/v1/system/status';

    const fetchStatus = async () => {
        try {
            const response = await fetch(apiUrl);
            if (!response.ok) {
                throw new Error(`Failed to fetch system status: ${response.status}`);
            }
            const data = await response.json();
            setSystemStatus(data.services || []);
            setError(null);
        } catch (e) {
            console.error(e);
            setError(e.message);
            // Don't clear status on error, so user can see last known state
        }
    };

    useEffect(() => {
        fetchStatus(); // Fetch immediately on load
        const interval = setInterval(fetchStatus, 5000); // Poll every 5 seconds
        return () => clearInterval(interval); // Cleanup on unmount
    }, []);

    const getServiceIcon = (serviceName) => {
        switch(serviceName.toLowerCase()) {
            case 'scheduler': return <Server size={24} color="#00f5ff" />;
            case 'frontend': return <Monitor size={24} color="#00f5ff" />;
            case 'redis': return <Database size={24} color="#00f5ff" />;
            case 'rabbitmq': return <Rabbit size={24} color="#00f5ff" />;
            default: return <Box size={24} color="#00f5ff" />;
        }
    };

    const getStatusIndicator = (status) => {
        const color = status === 'Running' ? '#00ff8c' : (status === 'Degraded' ? '#f59e0b' : '#ff4d4d');
        return (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ width: '10px', height: '10px', backgroundColor: color, borderRadius: '50%', boxShadow: `0 0 8px ${color}` }}></span>
                <span style={{ color: color, fontWeight: 'bold' }}>{status}</span>
            </div>
        );
    };

    return (
        <div style={styles.container}>
            <div style={styles.panel}>
                <h1 style={styles.title}><Monitor size={28} style={{ marginRight: '10px' }}/>System Health Monitor</h1>
                {error && <div style={styles.errorBox}>Failed to connect to backend: {error}</div>}
                <div style={styles.serviceGrid}>
                    {systemStatus.length > 0 ? (
                        systemStatus.map(service => (
                            <div key={service.name} style={styles.serviceCard}>
                                <div style={styles.serviceHeader}>
                                    {getServiceIcon(service.name)}
                                    <span style={styles.serviceName}>{service.name}</span>
                                </div>
                                <div style={styles.serviceInfo}>
                                    <strong>Status:</strong> {getStatusIndicator(service.status)}
                                </div>
                                <div style={styles.serviceInfo}>
                                    <strong>Pods:</strong> <span>{service.runningReplicas} / {service.desiredReplicas} Running</span>
                                </div>
                                <div style={styles.serviceInfo}>
                                    <strong>Pod Name:</strong> <span style={styles.podName}>{service.podName}</span>
                                </div>
                            </div>
                        ))
                    ) : (
                        !error && <Loader size={32} color="#00f5ff" className="animate-spin" style={{ margin: 'auto' }} />
                    )}
                </div>
            </div>
        </div>
    );
};
// --- End of Page 2 ---


// --- Main App Component (NEW) ---
// This component wraps everything and handles navigation
const App = () => {
    const [page, setPage] = useState('dag'); // 'dag' or 'system'

    const navButtonStyle = (active) => ({
        ...styles.navButton,
        backgroundColor: active ? '#00f5ff' : 'transparent',
        color: active ? '#101827' : '#00f5ff',
    });

    return (
        <div style={styles.mainApp}>
            <nav style={styles.navBar}>
                <span style={styles.navTitle}>Helios Orchestrator</span>
                <div>
                    <button style={navButtonStyle(page === 'dag')} onClick={() => setPage('dag')}>
                        <Terminal size={16} style={{ marginRight: '8px' }} />
                        DAG Monitor
                    </button>
                    <button style={navButtonStyle(page === 'system')} onClick={() => setPage('system')}>
                        <Monitor size={16} style={{ marginRight: '8px' }} />
                        System Health
                    </button>
                </div>
            </nav>
            {page === 'dag' && <DagMonitor />}
            {page === 'system' && <SystemMonitor />}
        </div>
    );
};
// --- End of Main App ---


// --- STYLES ---
// (Combined and updated styles for all components)
const styles = {
    // Main App & Nav
    mainApp: { width: '100%', minHeight: '100vh', backgroundColor: '#0a0f18', color: 'white' },
    navBar: {
        width: '90%', 
        maxWidth: '1200px', 
        margin: '0 auto', 
        padding: '20px 0', 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center',
        borderBottom: '1px solid #00f5ff'
    },
    navTitle: { fontSize: '24px', fontWeight: 'bold', color: '#00f5ff' },
    navButton: {
        border: '1px solid #00f5ff',
        padding: '10px 15px',
        borderRadius: '4px',
        cursor: 'pointer',
        marginLeft: '10px',
        fontWeight: 'bold',
        display: 'inline-flex',
        alignItems: 'center',
        transition: 'all 0.3s'
    },
    
    // Panels
    container: { width: '90%', maxWidth: '1200px', margin: '20px auto', fontFamily: 'Arial, sans-serif' },
    panel: { 
        width: '100%', 
        backgroundColor: '#101827', 
        border: '1px solid #00f5ff', 
        borderRadius: '8px', 
        padding: '30px', 
        boxShadow: '0 0 25px rgba(0, 245, 255, 0.3)', 
        boxSizing: 'border-box' 
    },
    title: { color: '#00f5ff', textAlign: 'center', margin: '0 0 20px 0', display: 'flex', alignItems: 'center', justifyContent: 'center' },
    
    // DAG Monitor
    editorContainer: { marginBottom: '20px' },
    textArea: { width: '100%', height: '300px', backgroundColor: '#0a0f18', border: '1px solid #334155', color: '#e0e0e0', borderRadius: '4px', padding: '15px', fontFamily: 'monospace', fontSize: '14px', boxSizing: 'border-box' },
    submitButton: { 
        width: '100%', 
        background: '#00f5ff', 
        border: 'none', 
        color: '#101827', 
        padding: '15px', 
        borderRadius: '4px', 
        fontSize: '18px', 
        fontWeight: 'bold', 
        cursor: 'pointer', 
        transition: 'opacity 0.2s',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '10px'
    },
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
        K8S_JOB_SUBMITTED: { color: '#00f5ff' },
        K8S_JOB_CREATING: { color: '#00f5ff' },
        QUEUED: { color: '#00f5ff' },
        PENDING: { color: '#94a3b8' },
        UPSTREAM_FAILED: { color: '#f59e0b' },
        DISPATCH_FAILED: { color: '#f59e0b' },
    },
    dependencies: { display: 'flex', alignItems: 'center', gap: '8px', color: '#94a3b8', fontSize: '12px', marginBottom: '10px', paddingLeft: '5px' },
    logs: { backgroundColor: '#0a0f18', padding: '10px', borderRadius: '4px', fontFamily: 'monospace', fontSize: '12px', color: '#e0e0e0', whiteSpace: 'pre-wrap', maxHeight: '150px', overflowY: 'auto', border: '1px solid #334155' },
    
    // System Monitor
    serviceGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
        gap: '20px',
    },
    serviceCard: {
        backgroundColor: '#1e293b',
        border: '1px solid #334155',
        borderRadius: '8px',
        padding: '20px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px'
    },
    serviceHeader: {
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        borderBottom: '1px solid #334155',
        paddingBottom: '12px'
    },
    serviceName: {
        color: 'white',
        fontSize: '20px',
        fontWeight: 'bold'
    },
    serviceInfo: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        color: '#cbd5e1',
        fontSize: '14px',
    },
    podName: {
        fontFamily: 'monospace',
        fontSize: '12px',
        color: '#94a3b8',
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis'
    }
};

export default App;