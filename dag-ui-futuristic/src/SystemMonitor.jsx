import React, { useState, useEffect } from 'react';
import { Server, Database, Rabbit, Box, Monitor, Loader } from 'lucide-react';

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
        }
    };

    useEffect(() => {
        fetchStatus();
        const interval = setInterval(fetchStatus, 5000);
        return () => clearInterval(interval);
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

export default SystemMonitor;

const styles = {
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
    errorBox: {
        backgroundColor: '#331d1d',
        color: '#ff4d4d',
        border: '1px solid #7f1d1d',
        padding: '15px',
        borderRadius: '6px',
        marginBottom: '20px',
        textAlign: 'center'
    },
    serviceGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
        gap: '20px'
    },
    serviceCard: {
        backgroundColor: '#0f172a',
        border: '1px solid #334155',
        borderRadius: '8px',
        padding: '20px',
        transition: 'all 0.3s',
        '&:hover': {
            borderColor: '#00f5ff',
            transform: 'translateY(-5px)'
        }
    },
    serviceHeader: {
        display: 'flex',
        alignItems: 'center',
        marginBottom: '15px'
    },
    serviceName: {
        fontSize: '18px',
        fontWeight: 'bold',
        marginLeft: '10px',
        color: '#e2e8f0'
    },
    serviceInfo: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '8px 0',
        borderBottom: '1px solid #1e293b',
        fontSize: '14px',
        color: '#94a3b8'
    },
};

