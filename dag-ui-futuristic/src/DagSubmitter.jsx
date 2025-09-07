import React, { useState, useRef } from 'react';
import jsyaml from 'js-yaml';
import { Edit3, Upload, FileText } from 'lucide-react';

const sampleYaml = `apiVersion: v1
dagName: "futuristic-pipeline"
tasks:
  - name: "initiate-telemetry"
    type: "command"
    image: "ubuntu:latest"
    command: ["/bin/bash", "-c", "echo 'Telemetry data stream initiated...'"]
`;

const DagSubmitter = () => {
    const [yamlText, setYamlText] = useState(sampleYaml);
    const [response, setResponse] = useState(null);
    const [error, setError] = useState('');
    const [activeTab, setActiveTab] = useState('editor');
    const [isDragging, setIsDragging] = useState(false);
    const fileInputRef = useRef(null);

    const handleSubmit = async () => {
        setResponse(null);
        setError('');
        if (!yamlText.trim()) {
            setError("Cannot submit an empty DAG.");
            return;
        }
        try {
            const dagObject = jsyaml.load(yamlText);
            // When running in Docker, we'll call the backend by its service name.
            // For local dev, it's localhost:8080. We can detect this.
            const apiUrl = process.env.NODE_ENV === 'production'
                ? '/api/v1/dags' // In production (Docker), this will be proxied
                : 'http://localhost:8080/api/v1/dags';

            const apiResponse = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(dagObject),
            });
            if (!apiResponse.ok) throw new Error(`Server Error: ${apiResponse.status}`);
            const result = await apiResponse.json();
            setResponse(result);
        } catch (err) {
            setError(err.message);
        }
    };

    const handleFileRead = (file) => {
        const reader = new FileReader();
        reader.onload = (e) => setYamlText(e.target.result);
        reader.onerror = (e) => setError("Failed to read file.");
        reader.readAsText(file);
    };

    const handleFileChange = (e) => handleFileRead(e.target.files[0]);
    const handleDrop = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setIsDragging(false);
        if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            handleFileRead(e.dataTransfer.files[0]);
            setActiveTab('editor');
        }
    };
    
    const dragProps = {
        onDragEnter: (e) => { e.preventDefault(); e.stopPropagation(); setIsDragging(true); },
        onDragLeave: (e) => { e.preventDefault(); e.stopPropagation(); setIsDragging(false); },
        onDragOver: (e) => { e.preventDefault(); e.stopPropagation(); },
        onDrop: handleDrop,
    };

    return (
        <div style={styles.container} {...dragProps}>
            <div style={styles.panel}>
                <h1 style={styles.title}>DAG Submission Terminal</h1>
                <div style={styles.tabs}>
                    <button onClick={() => setActiveTab('editor')} style={activeTab === 'editor' ? styles.tabActive : styles.tab}>
                        <Edit3 size={16} /> Editor
                    </button>
                    <button onClick={() => setActiveTab('upload')} style={activeTab === 'upload' ? styles.tabActive : styles.tab}>
                        <Upload size={16} /> Upload
                    </button>
                    <button onClick={() => setActiveTab('drag')} style={activeTab === 'drag' ? styles.tabActive : styles.tab}>
                        <FileText size={16} /> Drag & Drop
                    </button>
                </div>

                <div style={styles.content}>
                    {activeTab === 'editor' && <textarea style={styles.textArea} value={yamlText} onChange={(e) => setYamlText(e.target.value)} />}
                    {activeTab === 'upload' && (
                        <div style={styles.uploadBox} onClick={() => fileInputRef.current.click()}>
                            <Upload size={48} color="#00f5ff" />
                            <p>Click to select a .yml file</p>
                            <input type="file" ref={fileInputRef} onChange={handleFileChange} accept=".yml,.yaml" style={{ display: 'none' }} />
                        </div>
                    )}
                    {activeTab === 'drag' && (
                        <div style={{...styles.uploadBox, ...(isDragging ? styles.dragOver : {})}}>
                            <FileText size={48} color="#00f5ff" />
                            <p>Drop your .yml file here</p>
                        </div>
                    )}
                </div>

                <button style={styles.submitButton} onClick={handleSubmit}>Initiate Workflow</button>

                {response && (
                    <div style={styles.responseBox}>
                        <h3 style={{color: '#00ff8c'}}>Transmission Successful</h3>
                        <pre>{JSON.stringify(response, null, 2)}</pre>
                    </div>
                )}
                {error && (
                    <div style={{...styles.responseBox, borderColor: '#ff4d4d'}}>
                        <h3 style={{color: '#ff4d4d'}}>Transmission Error</h3>
                        <pre>{error}</pre>
                    </div>
                )}
            </div>
        </div>
    );
};

// CSS-in-JS for a futuristic look
const styles = {
    container: { minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' },
    panel: { width: '100%', maxWidth: '800px', backgroundColor: '#101827', border: '1px solid #00f5ff', borderRadius: '8px', padding: '30px', boxShadow: '0 0 25px rgba(0, 245, 255, 0.3)' },
    title: { color: '#00f5ff', textAlign: 'center', margin: '0 0 30px 0' },
    tabs: { display: 'flex', borderBottom: '1px solid #334155', marginBottom: '20px' },
    tab: { background: 'none', border: 'none', color: '#94a3b8', padding: '10px 20px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px' },
    tabActive: { background: 'none', border: 'none', color: '#00f5ff', padding: '10px 20px', cursor: 'pointer', borderBottom: '2px solid #00f5ff', display: 'flex', alignItems: 'center', gap: '8px' },
    content: { minHeight: '300px' },
    textArea: { width: '100%', height: '300px', backgroundColor: '#0a0f18', border: '1px solid #334155', color: '#e0e0e0', borderRadius: '4px', padding: '15px', fontFamily: 'monospace', fontSize: '14px', boxSizing: 'border-box' },
    uploadBox: { width: '100%', height: '300px', border: '2px dashed #334155', borderRadius: '4px', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', cursor: 'pointer', color: '#94a3b8', boxSizing: 'border-box' },
    dragOver: { borderColor: '#00f5ff', animation: 'glow 1.5s infinite alternate' },
    submitButton: { width: '100%', background: '#00f5ff', border: 'none', color: '#0a0f18', padding: '15px', borderRadius: '4px', fontSize: '18px', fontWeight: 'bold', cursor: 'pointer', marginTop: '20px' },
    responseBox: { marginTop: '20px', padding: '15px', backgroundColor: '#101827', border: '1px solid #00ff8c', borderRadius: '4px', whiteSpace: 'pre-wrap', fontFamily: 'monospace' },
};

export default DagSubmitter;
