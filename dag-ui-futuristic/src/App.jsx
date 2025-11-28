import React, { useState } from 'react';
import {
    Monitor, Terminal
} from 'lucide-react';
import DagMonitor from './DagSubmitter.jsx';

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

    React.useEffect(() => {
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
        const colorMap = {
            Healthy: '#00ff8c',
            Degraded: '#f59e0b',
            CrashLoop: '#ff4d4d',
            ImagePullError: '#ff4d4d',
            NotReady: '#f59e0b',
            Unresponsive: '#ff4d4d',
            Starting: '#94a3b8',
            Stopped: '#94a3b8',
            ScaledDown: '#94a3b8',
            Unknown: '#ff4d4d'
        };
        const color = colorMap[status] || '#94a3b8';
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
                                    <strong>Replicas:</strong> <span>{service.readyReplicas}/{service.desiredReplicas} Ready</span>
                                </div>
                                <div style={styles.serviceInfo}>
                                    <strong>Phase Summary:</strong>
                                    <span style={{ fontSize: '12px' }}>Running:{service.runningReplicas} Pending:{service.pendingReplicas} Crash:{service.crashLoopingReplicas} ImgErr:{service.imagePullBackOffReplicas}</span>
                                </div>
                                <div style={styles.serviceInfo}>
                                    <strong>First Pod:</strong> <span style={styles.podName}>{service.podName}</span>
                                </div>
                                {service.pods && service.pods.length > 0 && (
                                    <div style={{ marginTop: '6px', background: '#0a0f18', border: '1px solid #334155', borderRadius: '6px', padding: '8px', maxHeight: '180px', overflowY: 'auto' }}>
                                        {service.pods.map(p => (
                                            <div key={p.name} style={{ padding: '6px 0', borderBottom: '1px dashed #334155' }}>
                                                <div style={{ display: 'flex', justifyContent: 'space-between', fontFamily: 'monospace', fontSize: '12px' }}>
                                                    <span style={{ color: '#94a3b8', maxWidth: '55%' }}>{p.name}</span>
                                                    <span style={{ color: p.ready ? '#00ff8c' : '#f59e0b' }}>{p.phase}{p.reason ? ` (${p.reason})` : ''}</span>
                                                </div>
                                                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', marginTop: '4px' }}>
                                                    {p.trueConditions && p.trueConditions.map(c => (
                                                        <span key={c} style={{ background: '#1e293b', padding: '2px 6px', borderRadius: '4px', fontSize: '11px', color: '#00f5ff', border: '1px solid #334155' }}>{c}</span>
                                                    ))}
                                                    {p.containerWaitingReasons && p.containerWaitingReasons.map(r => (
                                                        <span key={r} style={{ background: '#331d1d', padding: '2px 6px', borderRadius: '4px', fontSize: '11px', color: '#ff4d4d', border: '1px solid #7f1d1d' }}>{r}</span>
                                                    ))}
                                                    <span style={{ background: '#0f172a', padding: '2px 6px', borderRadius: '4px', fontSize: '11px', color: '#94a3b8', border: '1px solid #334155' }}>restarts:{p.restarts}</span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
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


// --- Main App Component (UPDATED) ---
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

export default App;

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

