import React, { useState } from 'react';
import {
    Monitor, Terminal
} from 'lucide-react';
import DagMonitor from './DagSubmitter.jsx';
import SystemMonitor from './SystemMonitor.jsx';

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

