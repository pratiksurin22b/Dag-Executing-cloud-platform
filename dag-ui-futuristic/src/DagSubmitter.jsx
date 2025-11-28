import React, { useState, useRef, useEffect } from 'react';
import jsyaml from 'js-yaml';
// Import the new icon for the skipped/failed upstream status
import { Edit3, CheckCircle, XCircle, Loader, Clock, GitBranch, AlertTriangle, BarChart2, Activity } from 'lucide-react';

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
                {task.logs.map((log, index) => (
                    <div key={index}>{'> '}{log}</div>
                ))}
            </div>
        )}
    </div>
);

const DagMonitor = () => {
    const [yamlText, setYamlText] = useState(sampleYaml);
    const [dagState, setDagState] = useState(null);
    const [error, setError] = useState('');
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [useCache, setUseCache] = useState(true);

    const [metrics, setMetrics] = useState([]);
    const [metricsError, setMetricsError] = useState('');
    const [metricsLoading, setMetricsLoading] = useState(false);

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
            dagObject.useCache = useCache; // attach caching flag

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
            setMetrics([]);
            setMetricsError('');
        } catch (err) {
            setError(err.message);
        } finally {
            setIsSubmitting(false);
        }
    };

    const fetchMetrics = async () => {
        if (!dagState?.dagName) return;
        setMetricsLoading(true);
        setMetricsError('');
        try {
            const resp = await fetch(`${apiUrl}/${encodeURIComponent(dagState.dagName)}/metrics?limit=20`);
            if (!resp.ok) throw new Error(`Failed to load metrics: ${resp.status}`);
            const data = await resp.json();
            setMetrics(data || []);
        } catch (e) {
            setMetricsError(e.message);
        } finally {
            setMetricsLoading(false);
        }
    };

    const summarizeMetrics = (runs) => {
        const cached = runs.filter(r => String(r.cacheEnabled) === 'true');
        const nonCached = runs.filter(r => String(r.cacheEnabled) !== 'true');
        const avg = (arr) => arr.length ? (arr.reduce((s, r) => s + Number(r.durationMs || 0), 0) / arr.length) : 0;
        return {
            cachedCount: cached.length,
            nonCachedCount: nonCached.length,
            cachedAvgMs: Math.round(avg(cached)),
            nonCachedAvgMs: Math.round(avg(nonCached)),
        };
    };

    const metricsSummary = summarizeMetrics(metrics);

    const formatNodes = (nodesStr) => {
        if (!nodesStr) return '-';
        return String(nodesStr).split(',').filter(Boolean).join(', ');
    };

    const computeLoadFactor = (run) => {
        const total = Number(run.taskCount || 0);
        const succ = Number(run.taskSucceeded || 0);
        const failed = Number(run.taskFailed || 0) + Number(run.taskUpstreamFailed || 0);
        if (!total) return '-';
        return `${succ}/${total} ok, ${failed} failed`;
    };

    return (
        <div style={styles.container}>
            <div style={styles.panel}>
                <h1 style={styles.title}>DAG Orchestration Terminal</h1>
                <div style={styles.editorContainer}>
                    <textarea style={styles.textArea} value={yamlText} onChange={(e) => setYamlText(e.target.value)} />
                </div>

                <div style={styles.toggleRow}>
                    <label style={styles.toggleLabel}>
                        <input
                            type="checkbox"
                            checked={useCache}
                            onChange={e => setUseCache(e.target.checked)}
                            style={styles.checkbox}
                        />
                        <span>Enable node-level cache for this run</span>
                    </label>
                </div>

                <button style={styles.submitButton} onClick={handleSubmit} disabled={isSubmitting}>
                    {isSubmitting ? 'Submitting...' : 'Initiate Workflow'}
                </button>
                {error && <div style={styles.errorBox}>{error}</div>}
            </div>

            {dagState && (
                <>
                    <div style={{...styles.panel, marginTop: '20px'}}>
                        <h2 style={styles.title}>Live DAG Run: {dagState.dagName}</h2>
                        <p style={styles.dagId}>DAG Run ID: {dagState.dagId}</p>
                        <div style={styles.taskList}>
                            {dagState.tasks.map(task => <TaskItem key={task.name} task={task} />)}
                        </div>
                    </div>

                    <div style={{...styles.panel, marginTop: '20px'}}>
                        <div style={styles.metricsHeader}>
                            <h2 style={styles.title}><BarChart2 size={18} style={{marginRight: 8}} /> Run Metrics</h2>
                            <button style={styles.metricsButton} onClick={fetchMetrics} disabled={metricsLoading || !dagState?.dagName}>
                                {metricsLoading ? 'Loading...' : 'Load Metrics'}
                            </button>
                        </div>
                        {metricsError && <div style={styles.errorBox}>{metricsError}</div>}
                        {metrics.length === 0 && !metricsError && (
                            <p style={{color: '#94a3b8', fontSize: 14}}>No metrics available yet. Run a DAG and then click "Load Metrics".</p>
                        )}
                        {metrics.length > 0 && (
                            <>
                                <div style={styles.metricsSummaryRow}>
                                    <div style={styles.metricsCard}>
                                        <div style={styles.metricsLabel}>Cached runs</div>
                                        <div style={styles.metricsValue}>{metricsSummary.cachedCount}</div>
                                        <div style={styles.metricsHint}>Avg {metricsSummary.cachedAvgMs / 1000}s</div>
                                    </div>
                                    <div style={styles.metricsCard}>
                                        <div style={styles.metricsLabel}>Non-cached runs</div>
                                        <div style={styles.metricsValue}>{metricsSummary.nonCachedCount}</div>
                                        <div style={styles.metricsHint}>Avg {metricsSummary.nonCachedAvgMs / 1000}s</div>
                                    </div>
                                    <div style={styles.metricsCard}>
                                        <div style={styles.metricsLabel}>Task load (last run)</div>
                                        {metrics[0] && (
                                            <div style={styles.metricsValueSmall}>{computeLoadFactor(metrics[0])}</div>
                                        )}
                                        {!metrics[0] && <div style={styles.metricsHint}>-</div>}
                                    </div>
                                </div>
                                <div style={styles.metricsTableWrapper}>
                                    <table style={styles.metricsTable}>
                                        <thead>
                                        <tr>
                                            <th>Run ID</th>
                                            <th>Start Time</th>
                                            <th>Duration (s)</th>
                                            <th>Cache Enabled</th>
                                            <th>Cache Hit</th>
                                            <th>Status</th>
                                            <th>Tasks</th>
                                            <th>Nodes</th>
                                        </tr>
                                        </thead>
                                        <tbody>
                                        {metrics.map((m) => {
                                            const start = m.startTime ? new Date(Number(m.startTime)).toLocaleString() : '-';
                                            const durationSec = m.durationMs ? (Number(m.durationMs) / 1000).toFixed(2) : '-';
                                            const cached = String(m.cacheEnabled) === 'true';
                                            const hit = String(m.cacheHit) === 'true';
                                            const totalTasks = m.taskCount ?? '-';
                                            const load = computeLoadFactor(m);
                                            return (
                                                <tr key={m.dagId} style={cached ? styles.metricsRowCached : undefined}>
                                                    <td title={m.dagId}>{String(m.dagId).slice(0, 10)}...</td>
                                                    <td>{start}</td>
                                                    <td>{durationSec}</td>
                                                    <td>{cached ? 'Yes' : 'No'}</td>
                                                    <td>{hit ? 'Hit' : (cached ? 'Miss' : '-')}</td>
                                                    <td>{m.status || '-'}</td>
                                                    <td>{load}</td>
                                                    <td>{formatNodes(m.nodes)}</td>
                                                </tr>
                                            );
                                        })}
                                        </tbody>
                                    </table>
                                </div>
                            </>
                        )}
                    </div>
                </>
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
    toggleRow: { margin: '10px 0 20px', display: 'flex', justifyContent: 'flex-start' },
    toggleLabel: { display: 'flex', alignItems: 'center', gap: 8, color: '#e5e7eb', fontSize: 14 },
    checkbox: { marginRight: 6 },
    metricsHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 },
    metricsButton: { padding: '8px 14px', borderRadius: 4, border: '1px solid #00f5ff', background: 'transparent', color: '#00f5ff', cursor: 'pointer', fontSize: 13 },
    metricsSummaryRow: { display: 'flex', gap: 16, marginBottom: 16 },
    metricsCard: { flex: 1, backgroundColor: '#0b1220', borderRadius: 6, padding: 12, border: '1px solid #1f2937' },
    metricsLabel: { fontSize: 12, color: '#9ca3af', marginBottom: 4 },
    metricsValue: { fontSize: 20, fontWeight: 'bold', color: '#e5e7eb' },
    metricsValueSmall: { fontSize: 14, fontWeight: 'bold', color: '#e5e7eb' },
    metricsHint: { fontSize: 12, color: '#6b7280' },
    metricsTableWrapper: { overflowX: 'auto', marginTop: 8 },
    metricsTable: { width: '100%', borderCollapse: 'collapse', fontSize: 12, color: '#e5e7eb' },
    metricsRowCached: { backgroundColor: 'rgba(22, 163, 74, 0.09)' },
    // table cells
    'metricsTable th, metricsTable td': { borderBottom: '1px solid #1f2937', padding: '6px 8px', textAlign: 'left' },
};

export default DagMonitor;
