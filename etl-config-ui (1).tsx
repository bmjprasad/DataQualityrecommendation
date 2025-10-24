import React, { useState } from 'react';
import { Database, Check, RefreshCw, MessageSquare, Save, Plus, Trash2, AlertCircle, FileJson, ArrowLeftRight, Github } from 'lucide-react';

const ETLConfigBuilder = () => {
  const [activeTab, setActiveTab] = useState('source');
  const [showAIChat, setShowAIChat] = useState(false);
  const [showGitHubSync, setShowGitHubSync] = useState(false);
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState('');
  
  const [githubConfig, setGithubConfig] = useState({
    token: '',
    username: '',
    repository: '',
    branch: 'main',
    filePath: 'etl-configs/config.json',
    commitMessage: 'Update ETL configuration'
  });
  
  const [sourceConfig, setSourceConfig] = useState({
    sourceName: '',
    sourceType: 'database',
    connectionString: '',
    database: '',
    schema: '',
    table: '',
    query: '',
    fileFormat: 'parquet',
    filePath: ''
  });

  const [dataQualityChecks, setDataQualityChecks] = useState([
    { id: 1, checkType: 'null_check', column: '', filterBadRecords: false, errorLocation: '' }
  ]);

  const [transformations, setTransformations] = useState([
    { id: 1, transformType: 'select', expression: '', alias: '' }
  ]);

  const [syncConfig, setSyncConfig] = useState({
    targetName: '',
    targetType: 'delta',
    targetPath: '',
    writeMode: 'append',
    partitionBy: '',
    mergeKey: '',
    schedule: 'daily',
    scheduleTime: '00:00'
  });

  const addDataQualityCheck = () => {
    setDataQualityChecks([
      ...dataQualityChecks,
      { 
        id: Date.now(), 
        checkType: 'null_check', 
        column: '', 
        filterBadRecords: false, 
        errorLocation: '' 
      }
    ]);
  };

  const removeDataQualityCheck = (id) => {
    setDataQualityChecks(dataQualityChecks.filter(check => check.id !== id));
  };

  const updateDataQualityCheck = (id, field, value) => {
    setDataQualityChecks(dataQualityChecks.map(check =>
      check.id === id ? { ...check, [field]: value } : check
    ));
  };

  const addTransformation = () => {
    setTransformations([
      ...transformations,
      { id: Date.now(), transformType: 'select', expression: '', alias: '' }
    ]);
  };

  const removeTransformation = (id) => {
    setTransformations(transformations.filter(t => t.id !== id));
  };

  const updateTransformation = (id, field, value) => {
    setTransformations(transformations.map(t =>
      t.id === id ? { ...t, [field]: value } : t
    ));
  };

  const syncToGitHub = async () => {
    const fullConfig = {
      source: sourceConfig,
      dataQuality: dataQualityChecks,
      transformations: transformations,
      sync: syncConfig,
      metadata: {
        createdAt: new Date().toISOString(),
        createdBy: 'user@company.com',
        version: '1.0.0'
      }
    };

    // In production, this would call GitHub API
    console.log('Syncing to GitHub:', {
      repository: `${githubConfig.username}/${githubConfig.repository}`,
      branch: githubConfig.branch,
      filePath: githubConfig.filePath,
      config: fullConfig
    });

    // Simulate API call
    setTimeout(() => {
      alert(`Configuration synced to GitHub!\n\nRepository: ${githubConfig.username}/${githubConfig.repository}\nBranch: ${githubConfig.branch}\nFile: ${githubConfig.filePath}\n\nCommit: ${githubConfig.commitMessage}`);
      setShowGitHubSync(false);
    }, 1000);
  };

  const saveConfiguration = () => {
    const fullConfig = {
      source: sourceConfig,
      dataQuality: dataQualityChecks,
      transformations: transformations,
      sync: syncConfig,
      metadata: {
        createdAt: new Date().toISOString(),
        createdBy: 'user@company.com'
      }
    };
    
    console.log('Configuration to save to Delta Table:', fullConfig);
    alert('Configuration saved to Databricks Delta Table!\n\nTable: etl_framework.configurations\nPath: /mnt/delta/etl_configs/');
  };

  const handleAIChat = async () => {
    if (!chatInput.trim()) return;
    
    const userMessage = { role: 'user', content: chatInput };
    setChatMessages([...chatMessages, userMessage]);
    
    setTimeout(() => {
      const aiResponse = {
        role: 'assistant',
        content: `Based on your request "${chatInput}", I'll help generate the ETL configuration. Here's what I suggest:\n\nSource: Connect to your database using JDBC\nQuality Checks: Add null checks for critical columns\nTransformations: Apply standard cleansing rules\nSync: Write to Delta Lake with merge on primary key\n\nWould you like me to auto-populate these settings?`
      };
      setChatMessages(prev => [...prev, aiResponse]);
    }, 1000);
    
    setChatInput('');
  };

  const autoPopulateFromAI = () => {
    setSourceConfig({
      ...sourceConfig,
      sourceName: 'sales_data_prod',
      sourceType: 'database',
      database: 'prod_db',
      schema: 'sales',
      table: 'transactions'
    });
    
    setDataQualityChecks([
      { id: 1, checkType: 'null_check', column: 'customer_id', filterBadRecords: true, errorLocation: '/mnt/errors/null_records/' },
      { id: 2, checkType: 'range_check', column: 'amount', filterBadRecords: true, errorLocation: '/mnt/errors/range_violations/' }
    ]);
    
    alert('Configuration auto-populated from AI suggestions!');
    setShowAIChat(false);
  };

  const tabs = [
    { id: 'source', label: 'Source Configuration', icon: Database },
    { id: 'quality', label: 'Data Quality Checks', icon: Check },
    { id: 'transform', label: 'Transformations', icon: RefreshCw },
    { id: 'sync', label: 'Sync Details', icon: ArrowLeftRight }
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 p-6">
      <div className="max-w-7xl mx-auto">
        <div className="bg-white/10 backdrop-blur-lg rounded-2xl p-6 mb-6 border border-white/20">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-3xl font-bold text-white mb-2">ETL Framework Configuration Builder</h1>
              <p className="text-purple-200">Build and manage your data pipeline configurations</p>
            </div>
            <div className="flex gap-3">
              <button
                onClick={() => setShowGitHubSync(!showGitHubSync)}
                className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-gray-700 to-gray-900 text-white rounded-lg hover:from-gray-800 hover:to-black transition"
              >
                <Github size={20} />
                Sync to GitHub
              </button>
              <button
                onClick={() => setShowAIChat(!showAIChat)}
                className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-purple-500 to-pink-500 text-white rounded-lg hover:from-purple-600 hover:to-pink-600 transition"
              >
                <MessageSquare size={20} />
                AI Assistant
              </button>
              <button
                onClick={saveConfiguration}
                className="flex items-center gap-2 px-4 py-2 bg-green-500 text-white rounded-lg hover:bg-green-600 transition"
              >
                <Save size={20} />
                Save to Delta
              </button>
            </div>
          </div>
        </div>

        <div className="flex gap-6">
          <div className={`${showAIChat || showGitHubSync ? 'w-2/3' : 'w-full'} transition-all duration-300`}>
            <div className="bg-white/10 backdrop-blur-lg rounded-2xl p-2 mb-6 border border-white/20">
              <div className="flex gap-2">
                {tabs.map(tab => {
                  const Icon = tab.icon;
                  return (
                    <button
                      key={tab.id}
                      onClick={() => setActiveTab(tab.id)}
                      className={`flex items-center gap-2 px-4 py-3 rounded-lg transition flex-1 ${
                        activeTab === tab.id
                          ? 'bg-white text-purple-900 font-semibold'
                          : 'text-white hover:bg-white/10'
                      }`}
                    >
                      <Icon size={18} />
                      {tab.label}
                    </button>
                  );
                })}
              </div>
            </div>

            <div className="bg-white/10 backdrop-blur-lg rounded-2xl p-6 border border-white/20">
              {activeTab === 'source' && (
                <div className="space-y-4">
                  <h2 className="text-2xl font-bold text-white mb-4">Source Configuration</h2>
                  
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-purple-200 mb-2">Source Name</label>
                      <input
                        type="text"
                        value={sourceConfig.sourceName}
                        onChange={(e) => setSourceConfig({...sourceConfig, sourceName: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                        placeholder="my_data_source"
                      />
                    </div>
                    
                    <div>
                      <label className="block text-purple-200 mb-2">Source Type</label>
                      <select
                        value={sourceConfig.sourceType}
                        onChange={(e) => setSourceConfig({...sourceConfig, sourceType: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                      >
                        <option value="database">Database (JDBC)</option>
                        <option value="file">File (S3/ADLS)</option>
                        <option value="api">REST API</option>
                        <option value="kafka">Kafka Stream</option>
                      </select>
                    </div>
                  </div>

                  {sourceConfig.sourceType === 'database' && (
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-purple-200 mb-2">Connection String</label>
                        <input
                          type="text"
                          value={sourceConfig.connectionString}
                          onChange={(e) => setSourceConfig({...sourceConfig, connectionString: e.target.value})}
                          className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                          placeholder="jdbc:postgresql://host:5432/db"
                        />
                      </div>
                      
                      <div>
                        <label className="block text-purple-200 mb-2">Database</label>
                        <input
                          type="text"
                          value={sourceConfig.database}
                          onChange={(e) => setSourceConfig({...sourceConfig, database: e.target.value})}
                          className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                          placeholder="production_db"
                        />
                      </div>
                      
                      <div>
                        <label className="block text-purple-200 mb-2">Schema</label>
                        <input
                          type="text"
                          value={sourceConfig.schema}
                          onChange={(e) => setSourceConfig({...sourceConfig, schema: e.target.value})}
                          className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                          placeholder="public"
                        />
                      </div>
                      
                      <div>
                        <label className="block text-purple-200 mb-2">Table</label>
                        <input
                          type="text"
                          value={sourceConfig.table}
                          onChange={(e) => setSourceConfig({...sourceConfig, table: e.target.value})}
                          className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                          placeholder="users"
                        />
                      </div>
                    </div>
                  )}

                  {sourceConfig.sourceType === 'file' && (
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-purple-200 mb-2">File Format</label>
                        <select
                          value={sourceConfig.fileFormat}
                          onChange={(e) => setSourceConfig({...sourceConfig, fileFormat: e.target.value})}
                          className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                        >
                          <option value="parquet">Parquet</option>
                          <option value="csv">CSV</option>
                          <option value="json">JSON</option>
                          <option value="avro">Avro</option>
                        </select>
                      </div>
                      
                      <div>
                        <label className="block text-purple-200 mb-2">File Path</label>
                        <input
                          type="text"
                          value={sourceConfig.filePath}
                          onChange={(e) => setSourceConfig({...sourceConfig, filePath: e.target.value})}
                          className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                          placeholder="s3://bucket/data/*.parquet"
                        />
                      </div>
                    </div>
                  )}

                  <div>
                    <label className="block text-purple-200 mb-2">Custom SQL Query (Optional)</label>
                    <textarea
                      value={sourceConfig.query}
                      onChange={(e) => setSourceConfig({...sourceConfig, query: e.target.value})}
                      className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300 h-24"
                      placeholder="SELECT * FROM table WHERE condition..."
                    />
                  </div>
                </div>
              )}

              {activeTab === 'quality' && (
                <div className="space-y-4">
                  <div className="flex justify-between items-center mb-4">
                    <h2 className="text-2xl font-bold text-white">Data Quality Checks</h2>
                    <button
                      onClick={addDataQualityCheck}
                      className="flex items-center gap-2 px-4 py-2 bg-purple-500 text-white rounded-lg hover:bg-purple-600 transition"
                    >
                      <Plus size={18} />
                      Add Check
                    </button>
                  </div>

                  {dataQualityChecks.map((check, index) => (
                    <div key={check.id} className="bg-white/5 p-4 rounded-lg border border-white/20">
                      <div className="flex justify-between items-start mb-4">
                        <h3 className="text-lg font-semibold text-white">Check #{index + 1}</h3>
                        <button
                          onClick={() => removeDataQualityCheck(check.id)}
                          className="text-red-400 hover:text-red-300"
                        >
                          <Trash2 size={18} />
                        </button>
                      </div>

                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <label className="block text-purple-200 mb-2">Check Type</label>
                          <select
                            value={check.checkType}
                            onChange={(e) => updateDataQualityCheck(check.id, 'checkType', e.target.value)}
                            className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                          >
                            <option value="null_check">Null Check</option>
                            <option value="unique_check">Unique Check</option>
                            <option value="range_check">Range Check</option>
                            <option value="format_check">Format Check</option>
                            <option value="reference_check">Reference Check</option>
                            <option value="custom_check">Custom SQL Check</option>
                          </select>
                        </div>

                        <div>
                          <label className="block text-purple-200 mb-2">Column Name</label>
                          <input
                            type="text"
                            value={check.column}
                            onChange={(e) => updateDataQualityCheck(check.id, 'column', e.target.value)}
                            className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                            placeholder="column_name"
                          />
                        </div>
                      </div>

                      <div className="mt-4 p-4 bg-yellow-500/10 border border-yellow-500/30 rounded-lg">
                        <div className="flex items-start gap-3">
                          <AlertCircle className="text-yellow-400 mt-1" size={20} />
                          <div className="flex-1">
                            <label className="flex items-center gap-2 text-white mb-3 cursor-pointer">
                              <input
                                type="checkbox"
                                checked={check.filterBadRecords}
                                onChange={(e) => updateDataQualityCheck(check.id, 'filterBadRecords', e.target.checked)}
                                className="w-4 h-4"
                              />
                              <span className="font-semibold">Filter Bad Records</span>
                            </label>
                            
                            {check.filterBadRecords && (
                              <div>
                                <label className="block text-purple-200 mb-2 text-sm">Error Records Location</label>
                                <input
                                  type="text"
                                  value={check.errorLocation}
                                  onChange={(e) => updateDataQualityCheck(check.id, 'errorLocation', e.target.value)}
                                  className="w-full px-3 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300 text-sm"
                                  placeholder="/mnt/delta/errors/null_checks/"
                                />
                                <p className="text-xs text-purple-300 mt-1">
                                  Bad records will be written to this Delta table path for investigation
                                </p>
                              </div>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {activeTab === 'transform' && (
                <div className="space-y-4">
                  <div className="flex justify-between items-center mb-4">
                    <h2 className="text-2xl font-bold text-white">Transformations</h2>
                    <button
                      onClick={addTransformation}
                      className="flex items-center gap-2 px-4 py-2 bg-purple-500 text-white rounded-lg hover:bg-purple-600 transition"
                    >
                      <Plus size={18} />
                      Add Transformation
                    </button>
                  </div>

                  {transformations.map((transform, index) => (
                    <div key={transform.id} className="bg-white/5 p-4 rounded-lg border border-white/20">
                      <div className="flex justify-between items-start mb-4">
                        <h3 className="text-lg font-semibold text-white">Transform #{index + 1}</h3>
                        <button
                          onClick={() => removeTransformation(transform.id)}
                          className="text-red-400 hover:text-red-300"
                        >
                          <Trash2 size={18} />
                        </button>
                      </div>

                      <div className="grid grid-cols-1 gap-4">
                        <div>
                          <label className="block text-purple-200 mb-2">Transformation Type</label>
                          <select
                            value={transform.transformType}
                            onChange={(e) => updateTransformation(transform.id, 'transformType', e.target.value)}
                            className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                          >
                            <option value="select">Select Column</option>
                            <option value="cast">Cast Type</option>
                            <option value="filter">Filter Rows</option>
                            <option value="aggregate">Aggregate</option>
                            <option value="join">Join</option>
                            <option value="window">Window Function</option>
                            <option value="custom">Custom Expression</option>
                          </select>
                        </div>

                        <div>
                          <label className="block text-purple-200 mb-2">Expression / SQL</label>
                          <input
                            type="text"
                            value={transform.expression}
                            onChange={(e) => updateTransformation(transform.id, 'expression', e.target.value)}
                            className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                            placeholder="UPPER(name), amount * 1.1, etc."
                          />
                        </div>

                        <div>
                          <label className="block text-purple-200 mb-2">Alias (Optional)</label>
                          <input
                            type="text"
                            value={transform.alias}
                            onChange={(e) => updateTransformation(transform.id, 'alias', e.target.value)}
                            className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                            placeholder="column_alias"
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {activeTab === 'sync' && (
                <div className="space-y-4">
                  <h2 className="text-2xl font-bold text-white mb-4">Sync Details</h2>
                  
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-purple-200 mb-2">Target Name</label>
                      <input
                        type="text"
                        value={syncConfig.targetName}
                        onChange={(e) => setSyncConfig({...syncConfig, targetName: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                        placeholder="target_table"
                      />
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Target Type</label>
                      <select
                        value={syncConfig.targetType}
                        onChange={(e) => setSyncConfig({...syncConfig, targetType: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                      >
                        <option value="delta">Delta Lake</option>
                        <option value="parquet">Parquet</option>
                        <option value="database">Database</option>
                        <option value="iceberg">Iceberg</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Target Path / Table</label>
                      <input
                        type="text"
                        value={syncConfig.targetPath}
                        onChange={(e) => setSyncConfig({...syncConfig, targetPath: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                        placeholder="/mnt/delta/schema/table"
                      />
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Write Mode</label>
                      <select
                        value={syncConfig.writeMode}
                        onChange={(e) => setSyncConfig({...syncConfig, writeMode: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                      >
                        <option value="append">Append</option>
                        <option value="overwrite">Overwrite</option>
                        <option value="merge">Merge (Upsert)</option>
                        <option value="error">Error if Exists</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Partition By (Optional)</label>
                      <input
                        type="text"
                        value={syncConfig.partitionBy}
                        onChange={(e) => setSyncConfig({...syncConfig, partitionBy: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                        placeholder="date, region"
                      />
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Merge Key (for Upsert)</label>
                      <input
                        type="text"
                        value={syncConfig.mergeKey}
                        onChange={(e) => setSyncConfig({...syncConfig, mergeKey: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                        placeholder="id, composite_key"
                      />
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Schedule</label>
                      <select
                        value={syncConfig.schedule}
                        onChange={(e) => setSyncConfig({...syncConfig, schedule: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                      >
                        <option value="realtime">Real-time</option>
                        <option value="hourly">Hourly</option>
                        <option value="daily">Daily</option>
                        <option value="weekly">Weekly</option>
                        <option value="manual">Manual</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-purple-200 mb-2">Schedule Time</label>
                      <input
                        type="time"
                        value={syncConfig.scheduleTime}
                        onChange={(e) => setSyncConfig({...syncConfig, scheduleTime: e.target.value})}
                        className="w-full px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white"
                      />
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          {showAIChat && (
            <div className="w-1/3 bg-white/10 backdrop-blur-lg rounded-2xl border border-white/20 flex flex-col h-[calc(100vh-200px)]">
              <div className="p-4 border-b border-white/20">
                <h3 className="text-xl font-bold text-white flex items-center gap-2">
                  <MessageSquare size={20} />
                  AI Configuration Assistant
                </h3>
                <p className="text-sm text-purple-200 mt-1">Powered by Databricks AI</p>
              </div>

              <div className="flex-1 overflow-y-auto p-4 space-y-4">
                {chatMessages.length === 0 && (
                  <div className="text-center text-purple-300 mt-8">
                    <MessageSquare size={48} className="mx-auto mb-4 opacity-50" />
                    <p>Ask me to help configure your ETL pipeline!</p>
                    <p className="text-sm mt-2">Example: "Create a config to sync customer data from PostgreSQL to Delta Lake"</p>
                  </div>
                )}
                
                {chatMessages.map((msg, idx) => (
                  <div
                    key={idx}
                    className={`p-3 rounded-lg ${
                      msg.role === 'user'
                        ? 'bg-purple-500/30 ml-8'
                        : 'bg-white/20 mr-8'
                    }`}
                  >
                    <p className="text-white text-sm whitespace-pre-line">{msg.content}</p>
                  </div>
                ))}
              </div>

              {chatMessages.length > 0 && chatMessages[chatMessages.length - 1].role === 'assistant' && (
                <div className="p-4 border-t border-white/20">
                  <button
                    onClick={autoPopulateFromAI}
                    className="w-full px-4 py-2 bg-gradient-to-r from-green-500 to-emerald-500 text-white rounded-lg hover:from-green-600 hover:to-emerald-600 transition flex items-center justify-center gap-2"
                  >
                    <FileJson size={18} />
                    Apply AI Suggestions
                  </button>
                </div>
              )}

              <div className="p-4 border-t border-white/20">
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={chatInput}
                    onChange={(e) => setChatInput(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleAIChat()}
                    placeholder="Describe your ETL requirements..."
                    className="flex-1 px-4 py-2 bg-white/20 border border-white/30 rounded-lg text-white placeholder-purple-300"
                  />
                  <button
                    onClick={handleAIChat}
                    className="px-4 py-2 bg-purple-500 text-white rounded-lg hover:bg-purple-600 transition"
                  >
                    Send
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>

        {showGitHubSync && (
          <div className="fixed inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center z-50">
            <div className="bg-gradient-to-br from-slate-800 to-slate-900 rounded-2xl border border-white/20 p-6 w-full max-w-2xl mx-4">
              <div className="flex justify-between items-center mb-6">
                <h3 className="text-2xl font-bold text-white flex items-center gap-2">
                  <Github size={24} />
                  Sync Configuration to GitHub
                </h3>
                <button
                  onClick={() => setShowGitHubSync(false)}
                  className="text-gray-400 hover:text-white"
                >
                  ✕
                </button>
              </div>

              <div className="space-y-4">
                <div>
                  <label className="block text-purple-200 mb-2">GitHub Personal Access Token</label>
                  <input
                    type="password"
                    value={githubConfig.token}
                    onChange={(e) => setGithubConfig({...githubConfig, token: e.target.value})}
                    className="w-full px-4 py-2 bg-white/10 border border-white/30 rounded-lg text-white placeholder-purple-300"
                    placeholder="ghp_xxxxxxxxxxxxxxxxxxxx"
                  />
                  <p className="text-xs text-purple-300 mt-1">
                    Generate at: Settings → Developer settings → Personal access tokens
                  </p>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-purple-200 mb-2">GitHub Username</label>
                    <input
                      type="text"
                      value={githubConfig.username}
                      onChange={(e) => setGithubConfig({...githubConfig, username: e.target.value})}
                      className="w-full px-4 py-2 bg-white/10 border border-white/30 rounded-lg text-white placeholder-purple-300"
                      placeholder="your-username"
                    />
                  </div>

                  <div>
                    <label className="block text-purple-200 mb-2">Repository Name</label>
                    <input
                      type="text"
                      value={githubConfig.repository}
                      onChange={(e) => setGithubConfig({...githubConfig, repository: e.target.value})}
                      className="w-full px-4 py-2 bg-white/10 border border-white/30 rounded-lg text-white placeholder-purple-300"
                      placeholder="etl-configurations"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-purple-200 mb-2">Branch</label>
                    <input
                      type="text"
                      value={githubConfig.branch}
                      onChange={(e) => setGithubConfig({...githubConfig, branch: e.target.value})}
                      className="w-full px-4 py-2 bg-white/10 border border-white/30 rounded-lg text-white placeholder-purple-300"
                      placeholder="main"
                    />
                  </div>

                  <div>
                    <label className="block text-purple-200 mb-2">File Path</label>
                    <input
                      type="text"
                      value={githubConfig.filePath}
                      onChange={(e) => setGithubConfig({...githubConfig, filePath: e.target.value})}
                      className="w-full px-4 py-2 bg-white/10 border border-white/30 rounded-lg text-white placeholder-purple-300"
                      placeholder="configs/etl-config.json"
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-purple-200 mb-2">Commit Message</label>
                  <input
                    type="text"
                    value={githubConfig.commitMessage}
                    onChange={(e) => setGithubConfig({...githubConfig, commitMessage: e.target.value})}
                    className="w-full px-4 py-2 bg-white/10 border border-white/30 rounded-lg text-white placeholder-purple-300"
                    placeholder="Update ETL configuration"
                  />
                </div>

                <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
                  <h4 className="font-semibold text-white mb-2">Configuration Preview</h4>
                  <div className="text-sm text-purple-200 space-y-1">
                    <p>📁 Repository: {githubConfig.username}/{githubConfig.repository}</p>
                    <p>🌿 Branch: {githubConfig.branch}</p>
                    <p>📄 File: {githubConfig.filePath}</p>
                    <p>💾 Sources: {sourceConfig.sourceName || 'Not configured'}</p>
                    <p>✅ Quality Checks: {dataQualityChecks.length}</p>
                    <p>🔄 Transformations: {transformations.length}</p>
                  </div>
                </div>

                <div className="flex gap-3 pt-4">
                  <button
                    onClick={syncToGitHub}
                    className="flex-1 px-4 py-3 bg-gradient-to-r from-green-500 to-emerald-500 text-white rounded-lg hover:from-green-600 hover:to-emerald-600 transition font-semibold"
                  >
                    Sync to GitHub
                  </button>
                  <button
                    onClick={() => setShowGitHubSync(false)}
                    className="px-4 py-3 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ETLConfigBuilder;