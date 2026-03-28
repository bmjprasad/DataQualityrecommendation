import { useState, useCallback, useEffect, useRef, useMemo } from "react";

// ─── Constants ────────────────────────────────────────────────────────────────
const STEPS = [
  { id: "source", label: "Source", icon: "⬡", color: "#22d3ee" },
  { id: "dq", label: "Data Quality", icon: "◈", color: "#a78bfa" },
  { id: "transform", label: "Transformations", icon: "⬢", color: "#f472b6" },
  { id: "sink", label: "Main Sink", icon: "⬣", color: "#34d399" },
  { id: "extended", label: "Extended Sinks", icon: "⬡", color: "#fbbf24" },
];
const SOURCE_TYPES = [
  { value: "adls", label: "Azure Data Lake (ADLS)", fields: ["container", "path", "format", "delimiter"] },
  { value: "delta", label: "Delta Lake", fields: ["catalog", "schema", "table", "version"] },
  { value: "azuresql", label: "Azure SQL", fields: ["server", "database", "schema", "table", "query"] },
  { value: "cosmosdb", label: "Azure Cosmos DB", fields: ["endpoint", "database", "container", "partitionKey"] },
  { value: "db2", label: "IBM DB2", fields: ["host", "port", "database", "schema", "table"] },
  { value: "vsam", label: "VSAM / Mainframe", fields: ["dataset", "copybook", "encoding"] },
];
const SINK_TYPES = [
  { value: "delta", label: "Delta Lake", fields: ["catalog", "schema", "table", "mode", "partitionBy", "mergeKeys"] },
  { value: "adls", label: "Azure Data Lake (ADLS)", fields: ["container", "path", "format", "mode"] },
  { value: "azuresql", label: "Azure SQL", fields: ["server", "database", "schema", "table", "mode"] },
  { value: "cosmosdb", label: "Azure Cosmos DB", fields: ["endpoint", "database", "container", "mode"] },
  { value: "synapse", label: "Azure Synapse", fields: ["pool", "schema", "table", "mode", "distribution"] },
];
const DQ_RULE_TYPES = [
  { value: "not_null", label: "Not Null", params: [] },
  { value: "unique", label: "Unique", params: [] },
  { value: "range", label: "Range Check", params: ["min", "max"] },
  { value: "regex", label: "Regex Pattern", params: ["pattern"] },
  { value: "referential", label: "Referential Integrity", params: ["refTable", "refColumn"] },
  { value: "length", label: "String Length", params: ["minLen", "maxLen"] },
  { value: "enum", label: "Allowed Values", params: ["values"] },
];
const TRANSFORM_TYPES = [
  { value: "filter", label: "Filter", icon: "⊘", params: ["condition"] },
  { value: "derive", label: "Derive Column", icon: "＋", params: ["columnName", "expression"] },
  { value: "rename", label: "Rename", icon: "↔", params: ["from", "to"] },
  { value: "cast", label: "Type Cast", icon: "⇄", params: ["column", "targetType"] },
  { value: "aggregate", label: "Aggregate", icon: "Σ", params: ["groupBy", "aggregations"] },
  { value: "join", label: "Join", icon: "⋈", params: ["joinTable", "joinType", "joinCondition"] },
  { value: "sql", label: "Custom SQL", icon: "⌘", params: ["sqlExpression"] },
];
const TEMPLATES = [
  { id:"cdc",name:"CDC Incremental Load",desc:"Change data capture with merge into Delta",tags:["incremental","delta","merge"],source:"azuresql",sink:"delta"},
  { id:"full",name:"Full Refresh Dimension",desc:"Complete overwrite of dimension table",tags:["full","dimension","overwrite"],source:"db2",sink:"delta"},
  { id:"fanout",name:"Multi-Target Fan-Out",desc:"Single source to multiple sink targets",tags:["multi-target","fan-out"],source:"adls",sink:"delta"},
  { id:"cosmos",name:"Cosmos DB Sync",desc:"Delta Lake to Cosmos DB sync",tags:["cosmos","sync"],source:"delta",sink:"cosmosdb"},
  { id:"mainframe",name:"Mainframe Ingestion",desc:"VSAM/DB2 to Delta Lake",tags:["mainframe","vsam"],source:"vsam",sink:"delta"},
  { id:"lake",name:"Lake-to-Warehouse",desc:"ADLS raw to Synapse serving",tags:["warehouse","synapse"],source:"adls",sink:"synapse"},
];
const SAMPLE_COLUMNS = [
  {name:"customer_id",type:"INT"},{name:"first_name",type:"STRING"},{name:"last_name",type:"STRING"},
  {name:"email",type:"STRING"},{name:"phone",type:"STRING"},{name:"created_date",type:"TIMESTAMP"},
  {name:"status",type:"STRING"},{name:"balance",type:"DECIMAL(10,2)"},{name:"region_code",type:"STRING"},
  {name:"is_active",type:"BOOLEAN"},
];
const AI_EXAMPLES = [
  "Load customer data from Azure SQL with not-null checks on customer_id and email, filter active customers, merge into Delta Lake gold.dim_customer",
  "Ingest VSAM CUST.MASTER, validate phone regex, derive full_name, write to Delta silver.customer and sync to Cosmos DB",
  "CDC from DB2 ORDERS table, range check amount 0-999999, cast order_date to TIMESTAMP, merge into Delta gold.fact_orders",
  "Read parquet from ADLS raw-data/sales/, check uniqueness on txn_id, aggregate by region with sum of balance, overwrite Synapse dbo.regional_sales",
];

// ─── Existing Pipeline Catalog (simulated) ────────────────────────────────────
const EXISTING_PIPELINES = [
  {
    id:"pl-001",name:"cdc_customer_load",owner:"mrutyumjaya",team:"Data Engineering",
    platform:"databricks",status:"active",version:"v1.3",lastRun:"2026-03-28 06:15",
    lastRunStatus:"success",schedule:"Daily 6:00 AM",
    tags:["cdc","delta","production"],
    source:{type:"azuresql",fields:{server:"sqlprod.database.windows.net",database:"customers_db",schema:"dbo",table:"customers",query:""}},
    dqRules:[
      {id:"r1",column:"customer_id",ruleType:"not_null",params:{}},
      {id:"r2",column:"email",ruleType:"not_null",params:{}},
      {id:"r3",column:"email",ruleType:"regex",params:{pattern:"^[\\w.-]+@[\\w.-]+\\.\\w+$"}},
      {id:"r4",column:"balance",ruleType:"range",params:{min:"0",max:"999999"}},
    ],
    transforms:[
      {id:"t1",type:"filter",params:{condition:"is_active = true"}},
      {id:"t2",type:"derive",params:{columnName:"full_name",expression:"concat(first_name, ' ', last_name)"}},
      {id:"t3",type:"cast",params:{column:"created_date",targetType:"TIMESTAMP"}},
    ],
    sink:{type:"delta",fields:{catalog:"gold",schema:"customers",table:"dim_customer",mode:"merge",partitionBy:"region_code",mergeKeys:"customer_id"}},
    extendedSinks:[],
    versions:[
      {ver:"v1.3",date:"2026-03-27",author:"mrutyumjaya",changes:"+Added range check on balance\n+Added derive: full_name"},
      {ver:"v1.2",date:"2026-03-20",author:"mrutyumjaya",changes:"+Added email regex validation"},
      {ver:"v1.1",date:"2026-03-10",author:"data_team",changes:"+Changed sink mode to merge\n-Removed overwrite mode"},
      {ver:"v1.0",date:"2026-02-28",author:"mrutyumjaya",changes:"Initial pipeline creation"},
    ],
    runHistory:[
      {id:"run-10",date:"2026-03-28 06:15",status:"success",duration:"4m 32s",rowsRead:125430,rowsWritten:125430,dqPass:125430,dqFail:0},
      {id:"run-09",date:"2026-03-27 06:15",status:"success",duration:"4m 18s",rowsRead:124890,rowsWritten:124890,dqPass:124890,dqFail:0},
      {id:"run-08",date:"2026-03-26 06:15",status:"success",duration:"4m 45s",rowsRead:124320,rowsWritten:124320,dqPass:124318,dqFail:2},
      {id:"run-07",date:"2026-03-25 06:15",status:"failed",duration:"1m 02s",rowsRead:0,rowsWritten:0,dqPass:0,dqFail:0,error:"JDBC connection timeout to sqlprod.database.windows.net"},
      {id:"run-06",date:"2026-03-24 06:15",status:"success",duration:"4m 12s",rowsRead:123780,rowsWritten:123780,dqPass:123775,dqFail:5},
    ],
  },
  {
    id:"pl-002",name:"mainframe_ingest_orders",owner:"data_team",team:"Data Engineering",
    platform:"databricks",status:"active",version:"v2.1",lastRun:"2026-03-28 07:00",
    lastRunStatus:"success",schedule:"Daily 7:00 AM",tags:["mainframe","db2","production"],
    source:{type:"db2",fields:{host:"mainframe.corp.net",port:"50000",database:"PRODDB",schema:"ORDERS",table:"ORDER_MASTER"}},
    dqRules:[{id:"r1",column:"customer_id",ruleType:"not_null",params:{}},{id:"r2",column:"customer_id",ruleType:"unique",params:{}}],
    transforms:[{id:"t1",type:"cast",params:{column:"created_date",targetType:"TIMESTAMP"}}],
    sink:{type:"delta",fields:{catalog:"silver",schema:"orders",table:"order_master",mode:"merge",mergeKeys:"customer_id"}},
    extendedSinks:[],
    versions:[{ver:"v2.1",date:"2026-03-25",author:"data_team",changes:"+Changed mode to merge"},{ver:"v2.0",date:"2026-03-15",author:"data_team",changes:"Major refactor"}],
    runHistory:[
      {id:"run-20",date:"2026-03-28 07:00",status:"success",duration:"12m 05s",rowsRead:892340,rowsWritten:892340,dqPass:892340,dqFail:0},
      {id:"run-19",date:"2026-03-27 07:00",status:"success",duration:"11m 48s",rowsRead:891200,rowsWritten:891200,dqPass:891198,dqFail:2},
      {id:"run-18",date:"2026-03-26 07:00",status:"warning",duration:"13m 22s",rowsRead:890100,rowsWritten:889800,dqPass:889800,dqFail:300},
    ],
  },
  {
    id:"pl-003",name:"sales_fanout_weekly",owner:"analyst_a",team:"Analytics",
    platform:"synapse",status:"paused",version:"v1.0",lastRun:"2026-03-21 09:00",
    lastRunStatus:"failed",schedule:"Weekly Mon 9:00 AM",tags:["analytics","fan-out","synapse"],
    source:{type:"adls",fields:{container:"raw-data",path:"/landing/sales/",format:"parquet",delimiter:""}},
    dqRules:[{id:"r1",column:"customer_id",ruleType:"not_null",params:{}}],
    transforms:[{id:"t1",type:"aggregate",params:{groupBy:"region_code",aggregations:"sum(balance)"}}],
    sink:{type:"synapse",fields:{pool:"analytics",schema:"dbo",table:"regional_sales",mode:"overwrite",distribution:"HASH(region_code)"}},
    extendedSinks:[{id:"es1",type:"cosmosdb",fields:{endpoint:"cosmos.corp.net",database:"analytics",container:"sales",mode:"overwrite"}}],
    versions:[{ver:"v1.0",date:"2026-03-01",author:"analyst_a",changes:"Initial creation"}],
    runHistory:[
      {id:"run-30",date:"2026-03-21 09:00",status:"failed",duration:"0m 45s",rowsRead:0,rowsWritten:0,dqPass:0,dqFail:0,error:"Synapse pool 'analytics' is paused"},
      {id:"run-29",date:"2026-03-14 09:00",status:"success",duration:"8m 15s",rowsRead:45600,rowsWritten:45600,dqPass:45600,dqFail:0},
    ],
  },
  {
    id:"pl-004",name:"cosmos_product_sync",owner:"mrutyumjaya",team:"Data Engineering",
    platform:"databricks",status:"active",version:"v3.0",lastRun:"2026-03-28 08:30",
    lastRunStatus:"success",schedule:"Every 4 hours",tags:["cosmos","delta","sync"],
    source:{type:"delta",fields:{catalog:"gold",schema:"products",table:"dim_product",version:""}},
    dqRules:[{id:"r1",column:"customer_id",ruleType:"not_null",params:{}},{id:"r2",column:"status",ruleType:"enum",params:{values:"A,I,D"}}],
    transforms:[{id:"t1",type:"filter",params:{condition:"status = 'A'"}}],
    sink:{type:"cosmosdb",fields:{endpoint:"cosmos-prod.documents.azure.com",database:"products",container:"active_products",mode:"overwrite"}},
    extendedSinks:[],
    versions:[{ver:"v3.0",date:"2026-03-22",author:"mrutyumjaya",changes:"Switched to gold catalog source"}],
    runHistory:[
      {id:"run-40",date:"2026-03-28 08:30",status:"success",duration:"2m 10s",rowsRead:34500,rowsWritten:34500,dqPass:34500,dqFail:0},
      {id:"run-39",date:"2026-03-28 04:30",status:"success",duration:"2m 08s",rowsRead:34490,rowsWritten:34490,dqPass:34488,dqFail:2},
    ],
  },
];

const uid = () => Math.random().toString(36).slice(2, 9);

// ─── CSS ──────────────────────────────────────────────────────────────────────
const css = `
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap');
:root{--bg-root:#0a0e17;--bg-surface:#111827;--bg-card:#1a2234;--bg-input:#0f1729;--bg-hover:#1e2d45;--border:#1e2d45;--border-focus:#3b82f6;--text-primary:#e2e8f0;--text-secondary:#94a3b8;--text-muted:#475569;--accent-cyan:#22d3ee;--accent-purple:#a78bfa;--accent-pink:#f472b6;--accent-green:#34d399;--accent-amber:#fbbf24;--accent-blue:#3b82f6;--accent-red:#f87171;--radius:10px;--radius-sm:6px;}
*{margin:0;padding:0;box-sizing:border-box;}
body,#root{font-family:'DM Sans',sans-serif;background:var(--bg-root);color:var(--text-primary);min-height:100vh;overflow-x:hidden;}
.app{display:flex;flex-direction:column;height:100vh;overflow:hidden;background:radial-gradient(ellipse 80% 60% at 10% 0%,rgba(34,211,238,.04),transparent),radial-gradient(ellipse 60% 50% at 90% 100%,rgba(167,139,250,.03),transparent),var(--bg-root);}
.hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 20px;border-bottom:1px solid var(--border);background:rgba(17,24,39,.8);backdrop-filter:blur(12px);flex-shrink:0;gap:10px;flex-wrap:wrap;}
.logo{display:flex;align-items:center;gap:9px;font-weight:700;font-size:15px;letter-spacing:-.02em;}
.logo-icon{width:28px;height:28px;background:linear-gradient(135deg,var(--accent-cyan),var(--accent-purple));border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:13px;color:#000;font-weight:800;}
.logo-sub{font-size:9px;color:var(--text-muted);font-weight:400;letter-spacing:.04em;}
.hdr-actions{display:flex;gap:7px;align-items:center;flex-wrap:wrap;}
.btn{display:inline-flex;align-items:center;gap:5px;padding:6px 14px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg-card);color:var(--text-primary);font-family:'DM Sans',sans-serif;font-size:12px;font-weight:500;cursor:pointer;transition:all .2s;white-space:nowrap;}
.btn:hover{background:var(--bg-hover);border-color:var(--text-muted);}
.btn-primary{background:linear-gradient(135deg,var(--accent-blue),#6366f1);border-color:transparent;color:#fff;}
.btn-primary:hover{opacity:.9;transform:translateY(-1px);}
.btn-primary:disabled{opacity:.4;cursor:not-allowed;transform:none;}
.btn-accent{background:linear-gradient(135deg,var(--accent-cyan),#06b6d4);border-color:transparent;color:#0a0e17;font-weight:600;}
.btn-ghost{background:transparent;border-color:transparent;color:var(--text-secondary);}
.btn-ghost:hover{color:var(--text-primary);background:var(--bg-hover);}
.btn-sm{padding:4px 10px;font-size:11px;}
.btn-danger{border-color:#ef4444;color:#ef4444;background:transparent;}
.btn-danger:hover{background:rgba(239,68,68,.1);}
.btn-ai{background:linear-gradient(135deg,#6366f1,#a78bfa);border-color:transparent;color:#fff;font-weight:600;}
.btn-approve{background:linear-gradient(135deg,var(--accent-green),#10b981);border-color:transparent;color:#0a0e17;font-weight:600;}
.main{display:flex;flex:1;overflow:hidden;}
.mode-bar{display:flex;border-bottom:1px solid var(--border);background:var(--bg-surface);flex-shrink:0;padding:0 20px;overflow-x:auto;}
.mode-tab{padding:10px 16px;font-size:12px;font-weight:500;color:var(--text-muted);cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap;}
.mode-tab:hover{color:var(--text-secondary);}
.mode-tab.active{color:var(--text-primary);border-bottom-color:var(--accent-cyan);}
.step-nav{width:200px;border-right:1px solid var(--border);padding:16px 0;display:flex;flex-direction:column;gap:1px;background:rgba(17,24,39,.5);flex-shrink:0;}
.step-nav-hdr{padding:0 16px 12px;font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.1em;font-weight:600;}
.sni{display:flex;align-items:center;gap:9px;padding:9px 16px;cursor:pointer;transition:all .2s;position:relative;font-size:12px;color:var(--text-muted);}
.sni:hover{color:var(--text-secondary);background:var(--bg-hover);}
.sni.active{color:var(--text-primary);background:var(--bg-card);}
.sni.active::before{content:'';position:absolute;left:0;top:5px;bottom:5px;width:3px;border-radius:0 3px 3px 0;}
.sni-icon{width:26px;height:26px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:12px;background:var(--bg-input);border:1px solid var(--border);transition:all .2s;flex-shrink:0;}
.sni.active .sni-icon{border-color:currentColor;}
.sni.completed .sni-icon{background:rgba(34,211,238,.1);border-color:var(--accent-green);color:var(--accent-green);}
.sni-label{font-weight:500;font-size:12px;}
.sni-sub{font-size:9px;color:var(--text-muted);margin-top:1px;}
.content{flex:1;display:flex;flex-direction:column;overflow:hidden;}
.canvas{padding:14px 20px;border-bottom:1px solid var(--border);background:var(--bg-surface);flex-shrink:0;overflow-x:auto;}
.canvas-flow{display:flex;align-items:center;gap:0;min-width:max-content;}
.cn-node{display:flex;align-items:center;gap:8px;padding:8px 14px;border-radius:var(--radius);border:1.5px solid var(--border);background:var(--bg-card);cursor:pointer;transition:all .25s;min-width:130px;}
.cn-node:hover{transform:translateY(-2px);}
.cn-node.active{border-color:currentColor;box-shadow:0 0 14px currentColor;}
.cn-ico{width:28px;height:28px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;flex-shrink:0;}
.cn-lbl{font-size:10px;font-weight:600;}
.cn-det{font-size:9px;color:var(--text-muted);font-family:'JetBrains Mono',monospace;margin-top:1px;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.cn-conn{width:36px;height:2px;background:var(--border);position:relative;flex-shrink:0;}
.cn-conn::after{content:'▸';position:absolute;right:-3px;top:-9px;color:var(--text-muted);font-size:13px;}
.cn-conn.done{background:var(--accent-cyan);}
.cn-conn.done::after{color:var(--accent-cyan);}
.fpanel{flex:1;overflow-y:auto;padding:20px;}
.fpanel::-webkit-scrollbar{width:4px;}.fpanel::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.fsec{margin-bottom:20px;}
.fsec-title{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:12px;display:flex;align-items:center;gap:7px;}
.fsec-title::after{content:'';flex:1;height:1px;background:var(--border);}
.frow{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:12px;}
.ffield{display:flex;flex-direction:column;gap:4px;}
.flbl{font-size:10px;font-weight:500;color:var(--text-secondary);display:flex;align-items:center;gap:4px;}
.flbl .req{color:var(--accent-pink);}
.finput,.fsel{padding:7px 11px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg-input);color:var(--text-primary);font-family:'JetBrains Mono',monospace;font-size:11px;transition:border-color .2s;outline:none;width:100%;}
.finput:focus,.fsel:focus{border-color:var(--border-focus);box-shadow:0 0 0 2px rgba(59,130,246,.1);}
.finput::placeholder{color:var(--text-muted);}
.fsel{cursor:pointer;appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' fill='%2394a3b8'%3E%3Cpath d='M5 7L0 2h10z'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center;padding-right:24px;}
.fsel option{background:var(--bg-card);}
.ftxt{padding:7px 11px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg-input);color:var(--text-primary);font-family:'JetBrains Mono',monospace;font-size:11px;resize:vertical;min-height:60px;outline:none;width:100%;}
.ftxt:focus{border-color:var(--border-focus);}
.tgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:8px;margin-bottom:18px;}
.tcard{padding:12px;border:1.5px solid var(--border);border-radius:var(--radius);background:var(--bg-card);cursor:pointer;transition:all .2s;text-align:center;}
.tcard:hover{border-color:var(--text-muted);background:var(--bg-hover);transform:translateY(-1px);}
.tcard.sel{border-color:var(--accent-cyan);background:rgba(34,211,238,.05);}
.tcard-lbl{font-size:11px;font-weight:600;margin-top:3px;}
.tcard-ico{font-size:18px;opacity:.7;}
.rlist{display:flex;flex-direction:column;gap:7px;}
.ritem{display:flex;align-items:flex-start;gap:8px;padding:10px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);}
.ritem-fields{flex:1;display:flex;flex-wrap:wrap;gap:7px;align-items:center;}
.ritem-fields .fsel,.ritem-fields .finput{width:auto;min-width:120px;flex:1;}
.rsummary{font-size:10px;color:var(--accent-green);font-family:'JetBrains Mono',monospace;padding:4px 8px;background:rgba(52,211,153,.06);border-radius:var(--radius-sm);margin-top:5px;width:100%;}
.tlist{display:flex;flex-direction:column;gap:7px;}
.titem{padding:10px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);display:flex;flex-direction:column;gap:7px;}
.thdr{display:flex;align-items:center;justify-content:space-between;}
.tbadge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:14px;font-size:10px;font-weight:600;background:rgba(244,114,182,.1);color:var(--accent-pink);border:1px solid rgba(244,114,182,.2);}
.tfields{display:flex;flex-wrap:wrap;gap:7px;}
.tfields .finput,.tfields .ftxt{flex:1;min-width:180px;}
.schema{margin-top:12px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;}
.schema-hdr{padding:7px 12px;background:var(--bg-card);font-size:10px;font-weight:600;color:var(--text-secondary);display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);}
.schema-tbl{width:100%;font-size:10px;font-family:'JetBrains Mono',monospace;border-collapse:collapse;}
.schema-tbl th{padding:5px 12px;text-align:left;font-weight:500;color:var(--text-muted);background:var(--bg-surface);border-bottom:1px solid var(--border);font-size:9px;text-transform:uppercase;letter-spacing:.05em;}
.schema-tbl td{padding:4px 12px;border-bottom:1px solid rgba(30,45,69,.5);color:var(--text-secondary);}
.schema-tbl tr:hover td{background:var(--bg-hover);}
.col-type{color:var(--accent-purple);font-size:9px;}
.sink-cards{display:flex;flex-direction:column;gap:12px;}
.sink-card{padding:16px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);}
.sink-card-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;}
.sink-idx{width:20px;height:20px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;background:rgba(251,191,36,.1);color:var(--accent-amber);border:1px solid rgba(251,191,36,.2);}
.vbar{padding:7px 20px;display:flex;align-items:center;gap:7px;font-size:11px;flex-shrink:0;border-top:1px solid;}
.vbar.err{background:rgba(239,68,68,.06);border-top-color:rgba(239,68,68,.15);color:#f87171;}
.vbar.ok{background:rgba(52,211,153,.06);border-top-color:rgba(52,211,153,.15);color:var(--accent-green);}
.ftr{padding:10px 20px;border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;background:rgba(17,24,39,.5);flex-shrink:0;}
.badge{display:inline-flex;align-items:center;justify-content:center;min-width:15px;height:15px;border-radius:8px;font-size:9px;font-weight:700;background:var(--accent-cyan);color:#0a0e17;padding:0 4px;}
.empty{text-align:center;padding:36px 16px;color:var(--text-muted);}
.empty-ico{font-size:28px;margin-bottom:8px;opacity:.5;}
.empty-txt{font-size:12px;margin-bottom:12px;}
.cfg-drawer{position:fixed;right:0;top:0;bottom:0;width:380px;background:var(--bg-surface);border-left:1px solid var(--border);z-index:99;display:flex;flex-direction:column;box-shadow:-8px 0 30px rgba(0,0,0,.3);transform:translateX(100%);transition:transform .3s ease;}
.cfg-drawer.open{transform:translateX(0);}
.cfg-hdr{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-shrink:0;}
.cfg-hdr h3{font-size:12px;font-weight:600;}
.cfg-body{flex:1;overflow-y:auto;padding:12px;}
.cfg-body pre{font-family:'JetBrains Mono',monospace;font-size:10px;line-height:1.5;color:var(--text-secondary);white-space:pre-wrap;word-break:break-all;}
.ck{color:var(--accent-cyan);}.cv{color:var(--accent-green);}.cnj{color:var(--accent-amber);}.cp{color:var(--text-muted);}
.modal-ov{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:200;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px);}
.modal{background:var(--bg-surface);border:1px solid var(--border);border-radius:14px;max-height:85vh;overflow-y:auto;padding:22px;}
.modal-title{font-size:16px;font-weight:700;margin-bottom:4px;}
.modal-desc{font-size:12px;color:var(--text-secondary);margin-bottom:18px;}
.tmpgrid{display:grid;grid-template-columns:1fr 1fr;gap:9px;}
.tmpcard{padding:14px;border:1.5px solid var(--border);border-radius:var(--radius);background:var(--bg-card);cursor:pointer;transition:all .2s;}
.tmpcard:hover{border-color:var(--accent-cyan);background:var(--bg-hover);transform:translateY(-1px);}
.tmpcard-nm{font-size:12px;font-weight:600;margin-bottom:2px;}
.tmpcard-desc{font-size:10px;color:var(--text-secondary);margin-bottom:6px;}
.tags{display:flex;flex-wrap:wrap;gap:3px;}
.tag{padding:1px 6px;border-radius:7px;font-size:9px;background:rgba(34,211,238,.1);color:var(--accent-cyan);border:1px solid rgba(34,211,238,.15);}
.ai-panel{display:flex;flex-direction:column;height:100%;overflow:hidden;}
.ai-chat{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:12px;}
.ai-msg{max-width:85%;padding:12px 16px;border-radius:12px;font-size:12px;line-height:1.5;animation:fadeIn .3s ease;}
.ai-msg.user{align-self:flex-end;background:linear-gradient(135deg,#6366f1,#4f46e5);color:#fff;border-bottom-right-radius:4px;}
.ai-msg.bot{align-self:flex-start;background:var(--bg-card);border:1px solid var(--border);border-bottom-left-radius:4px;color:var(--text-secondary);}
.ai-config-preview{margin-top:10px;padding:10px;background:var(--bg-input);border-radius:7px;border:1px solid var(--border);font-family:'JetBrains Mono',monospace;font-size:10px;max-height:220px;overflow-y:auto;color:var(--text-secondary);}
.ai-actions{display:flex;gap:7px;margin-top:10px;}
.ai-input-area{padding:14px 20px;border-top:1px solid var(--border);background:rgba(17,24,39,.8);display:flex;gap:8px;flex-shrink:0;}
.ai-input{flex:1;padding:10px 14px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-input);color:var(--text-primary);font-family:'DM Sans',sans-serif;font-size:12px;outline:none;resize:none;min-height:40px;max-height:100px;}
.ai-input:focus{border-color:var(--accent-purple);}
.ai-input::placeholder{color:var(--text-muted);}
.ai-examples{display:flex;flex-wrap:wrap;gap:6px;padding:0 20px 12px;}
.ai-ex{padding:6px 12px;border:1px solid var(--border);border-radius:16px;font-size:10px;color:var(--text-secondary);cursor:pointer;transition:all .2s;background:var(--bg-card);max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.ai-ex:hover{border-color:var(--accent-purple);color:var(--text-primary);}
.ai-typing{display:flex;gap:3px;padding:3px 0;}
.ai-typing span{width:5px;height:5px;border-radius:50%;background:var(--accent-purple);animation:bounce .6s infinite alternate;}
.ai-typing span:nth-child(2){animation-delay:.2s;}
.ai-typing span:nth-child(3){animation-delay:.4s;}
.prog{height:3px;background:var(--bg-input);border-radius:2px;overflow:hidden;}
.prog-fill{height:100%;background:linear-gradient(90deg,var(--accent-cyan),var(--accent-purple));border-radius:2px;transition:width .4s ease;}
.prog-lbl{font-size:9px;color:var(--text-muted);margin-top:4px;}

/* ── Catalog ── */
.cat-panel{padding:20px;overflow-y:auto;height:100%;}
.cat-panel::-webkit-scrollbar{width:4px;}.cat-panel::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.cat-search{display:flex;gap:8px;margin-bottom:18px;}
.cat-search .finput{flex:1;padding:10px 14px;font-size:12px;font-family:'DM Sans',sans-serif;}
.cat-filters{display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap;}
.cat-filter{padding:5px 12px;border:1px solid var(--border);border-radius:16px;font-size:10px;cursor:pointer;transition:all .2s;color:var(--text-muted);background:transparent;}
.cat-filter:hover{border-color:var(--text-secondary);color:var(--text-secondary);}
.cat-filter.active{border-color:var(--accent-cyan);color:var(--accent-cyan);background:rgba(34,211,238,.06);}
.pl-card{border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);margin-bottom:10px;transition:all .2s;cursor:pointer;}
.pl-card:hover{border-color:var(--text-muted);transform:translateY(-1px);}
.pl-card-main{padding:16px;display:flex;gap:16px;align-items:flex-start;}
.pl-card-info{flex:1;min-width:0;}
.pl-card-name{font-size:14px;font-weight:600;font-family:'JetBrains Mono',monospace;margin-bottom:3px;}
.pl-card-meta{font-size:10px;color:var(--text-muted);display:flex;flex-wrap:wrap;gap:8px;margin-bottom:6px;}
.pl-card-tags{display:flex;gap:4px;flex-wrap:wrap;}
.pl-status{display:inline-flex;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;}
.pl-status.active{background:rgba(52,211,153,.1);color:var(--accent-green);border:1px solid rgba(52,211,153,.2);}
.pl-status.paused{background:rgba(251,191,36,.1);color:var(--accent-amber);border:1px solid rgba(251,191,36,.2);}
.pl-status.failed{background:rgba(239,68,68,.1);color:var(--accent-red);border:1px solid rgba(239,68,68,.2);}
.pl-run-status{font-size:10px;font-weight:600;}
.pl-run-status.success{color:var(--accent-green);}
.pl-run-status.failed{color:var(--accent-red);}
.pl-run-status.warning{color:var(--accent-amber);}
.pl-card-actions{display:flex;gap:6px;align-items:flex-start;flex-shrink:0;}
.pl-detail{border-top:1px solid var(--border);padding:16px;animation:fadeIn .2s ease;}
.pl-detail-tabs{display:flex;gap:0;margin-bottom:14px;border-bottom:1px solid var(--border);}
.pl-dtab{padding:7px 14px;font-size:11px;cursor:pointer;color:var(--text-muted);border-bottom:2px solid transparent;transition:all .2s;}
.pl-dtab:hover{color:var(--text-secondary);}
.pl-dtab.active{color:var(--text-primary);border-bottom-color:var(--accent-cyan);}
.ver-list{display:flex;flex-direction:column;gap:8px;}
.ver-item{display:flex;gap:12px;align-items:flex-start;padding:10px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg-input);}
.ver-badge{padding:2px 8px;border-radius:8px;font-size:10px;font-weight:600;background:rgba(34,211,238,.1);color:var(--accent-cyan);border:1px solid rgba(34,211,238,.15);white-space:nowrap;}
.ver-changes{font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--text-secondary);white-space:pre-wrap;}
.ver-changes .add{color:var(--accent-green);}
.ver-changes .rem{color:var(--accent-red);}
.run-tbl{width:100%;font-size:10px;border-collapse:collapse;}
.run-tbl th{padding:6px 10px;text-align:left;font-weight:500;color:var(--text-muted);background:var(--bg-surface);border-bottom:1px solid var(--border);text-transform:uppercase;font-size:9px;letter-spacing:.05em;}
.run-tbl td{padding:6px 10px;border-bottom:1px solid rgba(30,45,69,.5);color:var(--text-secondary);font-family:'JetBrains Mono',monospace;}
.run-tbl tr:hover td{background:var(--bg-hover);}

/* ── Import Modal ── */
.import-upload{border:2px dashed var(--border);border-radius:var(--radius);padding:36px 20px;text-align:center;cursor:pointer;transition:all .25s;background:var(--bg-card);}
.import-upload:hover{border-color:var(--accent-cyan);background:rgba(34,211,238,.02);}
.import-upload-ico{font-size:32px;margin-bottom:8px;opacity:.5;}
.import-upload-title{font-size:14px;font-weight:600;margin-bottom:4px;}
.import-upload-hint{font-size:11px;color:var(--text-muted);}
.import-preview{margin-top:18px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;max-height:340px;overflow-y:auto;}
.import-preview::-webkit-scrollbar{width:4px;}.import-preview::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.import-tbl{width:100%;font-size:10px;border-collapse:collapse;font-family:'JetBrains Mono',monospace;}
.import-tbl th{padding:8px 12px;text-align:left;font-weight:600;color:var(--text-muted);background:var(--bg-surface);border-bottom:1px solid var(--border);font-size:9px;text-transform:uppercase;letter-spacing:.05em;position:sticky;top:0;z-index:1;}
.import-tbl td{padding:6px 12px;border-bottom:1px solid rgba(30,45,69,.5);color:var(--text-secondary);}
.import-tbl tr:hover td{background:var(--bg-hover);}
.import-tbl .row-num{color:var(--text-muted);font-size:9px;width:28px;text-align:center;}
.import-tbl .mapped{color:var(--accent-green);}
.import-tbl .unmapped{color:var(--text-muted);font-style:italic;}
.import-map{margin-top:14px;display:flex;flex-wrap:wrap;gap:8px;}
.import-map-item{display:flex;align-items:center;gap:6px;padding:6px 10px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg-card);font-size:10px;}
.import-map-item .map-arrow{color:var(--accent-cyan);font-weight:700;}
.import-map-item .fsel{width:auto;min-width:100px;font-size:10px;padding:4px 8px;}
.import-summary{margin-top:16px;padding:14px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);display:flex;gap:20px;justify-content:center;}
.import-stat{text-align:center;}
.import-stat-val{font-size:22px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.import-stat-lbl{font-size:9px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.05em;margin-top:2px;}
.import-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:18px;}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);padding:10px 22px;border-radius:var(--radius);font-size:12px;font-weight:500;z-index:300;animation:slideUp .3s ease;pointer-events:none;}
.toast.success{background:var(--accent-green);color:#0a0e17;}
.toast.info{background:var(--accent-blue);color:#fff;}
@keyframes slideUp{from{opacity:0;transform:translate(-50%,16px)}to{opacity:1;transform:translate(-50%,0)}}

/* ── Observability ── */
.obs-panel{padding:20px;overflow-y:auto;height:100%;}
.obs-panel::-webkit-scrollbar{width:4px;}.obs-panel::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.obs-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:20px;}
.obs-metric{padding:16px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);}
.obs-metric-label{font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-weight:500;margin-bottom:6px;}
.obs-metric-value{font-size:24px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.obs-metric-sub{font-size:10px;color:var(--text-muted);margin-top:4px;}
.obs-metric-delta{font-size:10px;font-weight:600;margin-top:4px;display:inline-flex;align-items:center;gap:3px;padding:2px 6px;border-radius:8px;}
.obs-metric-delta.up{color:var(--accent-green);background:rgba(52,211,153,.1);}
.obs-metric-delta.down{color:var(--accent-red);background:rgba(248,113,113,.1);}
.obs-chart{border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);padding:16px;margin-bottom:16px;}
.obs-chart-title{font-size:12px;font-weight:600;margin-bottom:12px;display:flex;align-items:center;justify-content:space-between;}
.bar-chart{display:flex;align-items:flex-end;gap:4px;height:120px;padding-top:10px;}
.bar-col{display:flex;flex-direction:column;align-items:center;gap:3px;flex:1;}
.bar-rect{width:100%;border-radius:3px 3px 0 0;transition:height .5s ease;min-height:2px;position:relative;}
.bar-rect:hover{opacity:.85;}
.bar-label{font-size:8px;color:var(--text-muted);white-space:nowrap;}
.bar-val{font-size:8px;color:var(--text-secondary);font-family:'JetBrains Mono',monospace;}
.dq-score-ring{width:80px;height:80px;position:relative;}
.alert-list{display:flex;flex-direction:column;gap:8px;}
.alert-item{padding:12px;border:1px solid;border-radius:var(--radius);display:flex;gap:10px;align-items:flex-start;font-size:11px;}
.alert-item.critical{border-color:rgba(239,68,68,.3);background:rgba(239,68,68,.04);color:var(--accent-red);}
.alert-item.warning{border-color:rgba(251,191,36,.3);background:rgba(251,191,36,.04);color:var(--accent-amber);}
.alert-item.info{border-color:rgba(59,130,246,.3);background:rgba(59,130,246,.04);color:var(--accent-blue);}
.alert-item.resolved{border-color:rgba(52,211,153,.3);background:rgba(52,211,153,.04);color:var(--accent-green);}
.alert-ico{font-size:16px;flex-shrink:0;}
.alert-body{flex:1;}
.alert-title{font-weight:600;margin-bottom:2px;}
.alert-time{font-size:9px;opacity:.7;margin-top:3px;}
.sla-bar{display:flex;align-items:center;gap:10px;padding:10px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);margin-bottom:8px;}
.sla-name{font-size:11px;font-weight:500;width:180px;font-family:'JetBrains Mono',monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.sla-track{flex:1;height:8px;background:var(--bg-input);border-radius:4px;overflow:hidden;}
.sla-fill{height:100%;border-radius:4px;transition:width .5s ease;}
.sla-pct{font-size:11px;font-weight:600;width:50px;text-align:right;font-family:'JetBrains Mono',monospace;}

/* Approval */
.appr-card{padding:16px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);margin-bottom:10px;}
.appr-hdr{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;}
.appr-status{display:inline-flex;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600;}
.appr-status.pending{background:rgba(251,191,36,.1);color:var(--accent-amber);border:1px solid rgba(251,191,36,.2);}
.appr-status.approved{background:rgba(52,211,153,.1);color:var(--accent-green);border:1px solid rgba(52,211,153,.2);}
.appr-status.rejected{background:rgba(239,68,68,.1);color:var(--accent-red);border:1px solid rgba(239,68,68,.2);}
.appr-diff{background:var(--bg-input);border:1px solid var(--border);border-radius:6px;padding:10px;font-family:'JetBrains Mono',monospace;font-size:10px;color:var(--text-secondary);max-height:160px;overflow-y:auto;margin-bottom:10px;}
.appr-diff .add{color:var(--accent-green);}.appr-diff .rem{color:var(--accent-red);}
.timeline{display:flex;flex-direction:column;gap:0;margin-top:16px;}
.tl-item{display:flex;gap:12px;padding-bottom:16px;position:relative;}
.tl-item::before{content:'';position:absolute;left:10px;top:22px;bottom:0;width:1px;background:var(--border);}
.tl-item:last-child::before{display:none;}
.tl-dot{width:20px;height:20px;border-radius:50%;border:2px solid var(--border);background:var(--bg-surface);display:flex;align-items:center;justify-content:center;font-size:9px;flex-shrink:0;z-index:1;}
.tl-dot.green{border-color:var(--accent-green);color:var(--accent-green);}
.tl-dot.amber{border-color:var(--accent-amber);color:var(--accent-amber);}
.tl-dot.blue{border-color:var(--accent-blue);color:var(--accent-blue);}
.tl-content{font-size:11px;color:var(--text-secondary);}
.tl-content strong{color:var(--text-primary);font-weight:600;}
.tl-time{font-size:9px;color:var(--text-muted);margin-top:1px;}

/* Lineage */
.lineage-panel{padding:20px;overflow:auto;height:100%;}
.lin-stage{fill:var(--bg-card);stroke:var(--border);stroke-width:1.5;rx:10;}
.lin-label{fill:var(--text-primary);font-family:'DM Sans',sans-serif;font-size:11px;font-weight:600;}
.lin-col{fill:var(--text-secondary);font-family:'JetBrains Mono',monospace;font-size:9px;}
.lin-line{stroke-width:1.5;fill:none;}
.lin-type{fill:var(--accent-purple);font-family:'JetBrains Mono',monospace;font-size:8px;}

@keyframes fadeIn{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}.fade-in{animation:fadeIn .25s ease forwards;}
@keyframes bounce{to{transform:translateY(-5px);opacity:.4;}}

/* ── Data Discovery ── */
.disc-panel{padding:20px;overflow-y:auto;height:100%;}
.disc-panel::-webkit-scrollbar{width:4px;}.disc-panel::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.disc-search{display:flex;gap:8px;margin-bottom:16px;}
.disc-search .finput{flex:1;padding:10px 14px;font-size:12px;font-family:'DM Sans',sans-serif;}
.disc-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-bottom:18px;}
.disc-stat{padding:12px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);text-align:center;}
.disc-stat-val{font-size:22px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.disc-stat-lbl{font-size:9px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;margin-top:2px;}
.disc-layout{display:flex;gap:16px;height:calc(100% - 180px);min-height:400px;}
.disc-sidebar{width:260px;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);display:flex;flex-direction:column;overflow:hidden;flex-shrink:0;}
.disc-sidebar-hdr{padding:10px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;display:flex;align-items:center;justify-content:space-between;}
.disc-tree{flex:1;overflow-y:auto;padding:6px 0;}
.disc-tree::-webkit-scrollbar{width:3px;}.disc-tree::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.disc-tree-group{margin-bottom:2px;}
.disc-tree-group-hdr{padding:6px 14px;font-size:10px;font-weight:600;color:var(--text-muted);cursor:pointer;display:flex;align-items:center;gap:6px;transition:color .15s;}
.disc-tree-group-hdr:hover{color:var(--text-secondary);}
.disc-tree-item{padding:7px 14px 7px 30px;font-size:11px;font-family:'JetBrains Mono',monospace;cursor:pointer;transition:all .15s;display:flex;align-items:center;gap:8px;color:var(--text-secondary);}
.disc-tree-item:hover{background:var(--bg-hover);color:var(--text-primary);}
.disc-tree-item.active{background:rgba(34,211,238,.06);color:var(--accent-cyan);border-right:2px solid var(--accent-cyan);}
.disc-tier{display:inline-flex;padding:1px 6px;border-radius:6px;font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;}
.disc-tier.bronze{background:rgba(180,130,80,.15);color:#c89050;border:1px solid rgba(180,130,80,.25);}
.disc-tier.silver{background:rgba(148,163,184,.12);color:var(--text-secondary);border:1px solid rgba(148,163,184,.2);}
.disc-tier.gold{background:rgba(251,191,36,.1);color:var(--accent-amber);border:1px solid rgba(251,191,36,.2);}
.disc-main{flex:1;border:1px solid var(--border);border-radius:var(--radius);background:var(--bg-card);display:flex;flex-direction:column;overflow:hidden;}
.disc-main-hdr{padding:14px 18px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;justify-content:space-between;flex-shrink:0;}
.disc-main-name{font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;}
.disc-main-meta{font-size:10px;color:var(--text-muted);display:flex;flex-wrap:wrap;gap:10px;margin-top:4px;}
.disc-tabs{display:flex;border-bottom:1px solid var(--border);flex-shrink:0;}
.disc-tab{padding:8px 16px;font-size:11px;font-weight:500;color:var(--text-muted);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;}
.disc-tab:hover{color:var(--text-secondary);}
.disc-tab.active{color:var(--text-primary);border-bottom-color:var(--accent-cyan);}
.disc-body{flex:1;overflow-y:auto;padding:16px 18px;}
.disc-body::-webkit-scrollbar{width:3px;}.disc-body::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px;}
.disc-col-tbl{width:100%;font-size:10px;border-collapse:collapse;}
.disc-col-tbl th{padding:7px 12px;text-align:left;font-weight:600;color:var(--text-muted);background:var(--bg-surface);border-bottom:1px solid var(--border);font-size:9px;text-transform:uppercase;letter-spacing:.05em;}
.disc-col-tbl td{padding:6px 12px;border-bottom:1px solid rgba(30,45,69,.5);color:var(--text-secondary);font-family:'JetBrains Mono',monospace;}
.disc-col-tbl tr:hover td{background:var(--bg-hover);}
.pii-badge{display:inline-flex;padding:1px 6px;border-radius:6px;font-size:8px;font-weight:600;background:rgba(244,114,182,.12);color:var(--accent-pink);border:1px solid rgba(244,114,182,.2);}
.dq-badge{display:inline-flex;padding:1px 6px;border-radius:6px;font-size:8px;font-weight:600;background:rgba(167,139,250,.1);color:var(--accent-purple);border:1px solid rgba(167,139,250,.2);}
.disc-lineage-item{display:flex;align-items:center;gap:10px;padding:10px 12px;border:1px solid var(--border);border-radius:var(--radius-sm);background:var(--bg-input);margin-bottom:6px;}
.disc-lineage-dir{font-size:9px;font-weight:700;padding:2px 8px;border-radius:8px;}
.disc-lineage-dir.read{background:rgba(34,211,238,.1);color:var(--accent-cyan);border:1px solid rgba(34,211,238,.2);}
.disc-lineage-dir.write{background:rgba(52,211,153,.1);color:var(--accent-green);border:1px solid rgba(52,211,153,.2);}
.disc-lineage-dir.readwrite{background:rgba(251,191,36,.1);color:var(--accent-amber);border:1px solid rgba(251,191,36,.2);}
.disc-empty{text-align:center;padding:40px;color:var(--text-muted);font-size:12px;}
`;

// ─── Config Generator ─────────────────────────────────────────────────────────
function generateConfig(c) {
  const out = {pipeline:{name:c.pipelineName||"untitled",version:"1.0",engine:"spark-scala",platform:c.platform||"databricks"},
    source:c.source?.type?{type:c.source.type,...c.source.fields}:undefined,
    dataQuality:c.dqRules?.length>0?{rules:c.dqRules.map(r=>({column:r.column,check:r.ruleType,...(Object.keys(r.params||{}).length>0?{params:r.params}:{})}))}:undefined,
    transformations:c.transforms?.length>0?c.transforms.map(t=>({type:t.type,...t.params})):undefined,
    sink:c.sink?.type?{type:c.sink.type,...c.sink.fields}:undefined,
    extendedSinks:c.extendedSinks?.length>0?c.extendedSinks.filter(s=>s.type).map(s=>({type:s.type,...s.fields})):undefined};
  Object.keys(out).forEach(k=>out[k]===undefined&&delete out[k]); return out;
}
function ConfigJSON({config}){ const j=JSON.stringify(generateConfig(config),null,2); const h=j.replace(/"([^"]+)":/g,'<span class="ck">"$1"</span>:').replace(/: "([^"]+)"/g,': <span class="cv">"$1"</span>').replace(/: (\d+)/g,': <span class="cnj">$1</span>').replace(/[{}[\]]/g,'<span class="cp">$&</span>'); return <pre dangerouslySetInnerHTML={{__html:h}}/>;}

// ─── Source Form ──────────────────────────────────────────────────────────────
function SourceForm({config,setConfig,columns}){
  const sel=SOURCE_TYPES.find(s=>s.value===config.source?.type);
  const setSrc=(u)=>setConfig(c=>({...c,source:{...c.source,...u}}));
  const setF=(f,v)=>setSrc({fields:{...config.source?.fields,[f]:v}});
  return(<div className="fade-in">
    <div className="fsec"><div className="fsec-title">Select Source Type</div>
      <div className="tgrid">{SOURCE_TYPES.map(st=><div key={st.value} className={`tcard ${config.source?.type===st.value?'sel':''}`} onClick={()=>setSrc({type:st.value,fields:{}})}>
        <div className="tcard-ico">{st.value==='adls'?'☁':st.value==='delta'?'△':st.value==='azuresql'?'⊞':st.value==='cosmosdb'?'◎':st.value==='db2'?'⬡':'▤'}</div><div className="tcard-lbl">{st.label}</div></div>)}</div></div>
    {sel&&<div className="fsec fade-in"><div className="fsec-title">Connection Details</div>
      <div className="frow">{sel.fields.map(f=><div className="ffield" key={f}><label className="flbl">{f.replace(/([A-Z])/g,' $1').replace(/^./,s=>s.toUpperCase())} <span className="req">*</span></label>
        {f==='query'?<textarea className="ftxt" placeholder={f} value={config.source?.fields?.[f]||''} onChange={e=>setF(f,e.target.value)}/>:
        f==='format'?<select className="fsel" value={config.source?.fields?.[f]||''} onChange={e=>setF(f,e.target.value)}><option value="">Select</option><option>parquet</option><option>csv</option><option>json</option><option>avro</option><option>orc</option></select>:
        <input className="finput" placeholder={f} value={config.source?.fields?.[f]||''} onChange={e=>setF(f,e.target.value)}/>}</div>)}</div>
      <button className="btn btn-sm" style={{color:'var(--accent-cyan)'}}>⚡ Test Connection</button></div>}
    {sel&&<div className="schema fade-in"><div className="schema-hdr"><span>Schema ({columns.length} cols)</span><span style={{color:'var(--accent-green)',fontSize:9}}>● Simulated</span></div>
      <table className="schema-tbl"><thead><tr><th>#</th><th>Column</th><th>Type</th></tr></thead><tbody>{columns.map((c,i)=><tr key={c.name}><td style={{color:'var(--text-muted)'}}>{i+1}</td><td>{c.name}</td><td><span className="col-type">{c.type}</span></td></tr>)}</tbody></table></div>}
  </div>);
}

// ─── DQ Form ──────────────────────────────────────────────────────────────────
function DQForm({config,setConfig,columns}){
  const rules=config.dqRules||[];
  const add=()=>setConfig(c=>({...c,dqRules:[...(c.dqRules||[]),{id:uid(),column:'',ruleType:'',params:{}}]}));
  const upd=(id,u)=>setConfig(c=>({...c,dqRules:c.dqRules.map(r=>r.id===id?{...r,...u}:r)}));
  const del=(id)=>setConfig(c=>({...c,dqRules:c.dqRules.filter(r=>r.id!==id)}));
  const summary=(r)=>{if(!r.column||!r.ruleType)return null;const rt=DQ_RULE_TYPES.find(x=>x.value===r.ruleType);let s=`"${r.column}" → ${rt?.label}`;if(r.ruleType==='range')s+=` [${r.params?.min||'?'}–${r.params?.max||'?'}]`;if(r.ruleType==='regex')s+=` /${r.params?.pattern||'...'}/`;return s;};
  return(<div className="fade-in"><div className="fsec"><div className="fsec-title">Data Quality Rules <span className="badge">{rules.length}</span></div>
    <div className="rlist">{rules.map(r=>{const rt=DQ_RULE_TYPES.find(x=>x.value===r.ruleType);return(
      <div className="ritem fade-in" key={r.id}><div style={{flex:1}}><div className="ritem-fields">
        <select className="fsel" value={r.column} onChange={e=>upd(r.id,{column:e.target.value})}><option value="">Column</option>{columns.map(c=><option key={c.name} value={c.name}>{c.name}</option>)}</select>
        <select className="fsel" value={r.ruleType} onChange={e=>upd(r.id,{ruleType:e.target.value,params:{}})}><option value="">Rule</option>{DQ_RULE_TYPES.map(x=><option key={x.value} value={x.value}>{x.label}</option>)}</select>
        {rt?.params.map(p=><input key={p} className="finput" placeholder={p} value={r.params?.[p]||''} onChange={e=>upd(r.id,{params:{...r.params,[p]:e.target.value}})} style={{maxWidth:120}}/>)}
      </div>{summary(r)&&<div className="rsummary">✓ {summary(r)}</div>}</div>
      <button className="btn btn-sm btn-danger" onClick={()=>del(r.id)}>✕</button></div>);})}</div>
    <div style={{marginTop:10}}><button className="btn btn-sm" onClick={add}>+ Add Rule</button></div>
    {rules.length===0&&<div className="empty"><div className="empty-ico">◈</div><div className="empty-txt">No rules yet</div><button className="btn btn-sm" onClick={add}>+ Add Rule</button></div>}
  </div></div>);
}

// ─── Transform Form ───────────────────────────────────────────────────────────
function TransformForm({config,setConfig,columns}){
  const xf=config.transforms||[];
  const add=(type)=>setConfig(c=>({...c,transforms:[...(c.transforms||[]),{id:uid(),type,params:{}}]}));
  const upd=(id,u)=>setConfig(c=>({...c,transforms:c.transforms.map(t=>t.id===id?{...t,...u}:t)}));
  const del=(id)=>setConfig(c=>({...c,transforms:c.transforms.filter(t=>t.id!==id)}));
  return(<div className="fade-in"><div className="fsec"><div className="fsec-title">Transforms <span className="badge">{xf.length}</span></div>
    <div style={{display:'flex',flexWrap:'wrap',gap:5,marginBottom:14}}>{TRANSFORM_TYPES.map(tt=><button key={tt.value} className="btn btn-sm" onClick={()=>add(tt.value)}><span style={{opacity:.7}}>{tt.icon}</span> {tt.label}</button>)}</div>
    <div className="tlist">{xf.map((t,idx)=>{const tt=TRANSFORM_TYPES.find(x=>x.value===t.type);return(
      <div className="titem fade-in" key={t.id}><div className="thdr"><div style={{display:'flex',alignItems:'center',gap:7}}><span style={{color:'var(--text-muted)',fontSize:10,fontFamily:'JetBrains Mono'}}>#{idx+1}</span><span className="tbadge">{tt?.icon} {tt?.label}</span></div>
        <button className="btn btn-sm btn-danger" onClick={()=>del(t.id)}>✕</button></div>
      <div className="tfields">{tt?.params.map(p=>
        (p==='sqlExpression'||p==='condition'||p==='joinCondition')?<textarea key={p} className="ftxt" placeholder={p} value={t.params?.[p]||''} onChange={e=>upd(t.id,{params:{...t.params,[p]:e.target.value}})} style={{minHeight:50}}/>:
        (p==='column'||p==='from'||p==='groupBy')?<select key={p} className="fsel" value={t.params?.[p]||''} onChange={e=>upd(t.id,{params:{...t.params,[p]:e.target.value}})} style={{minWidth:140}}><option value="">{p}</option>{columns.map(c=><option key={c.name} value={c.name}>{c.name}</option>)}</select>:
        p==='targetType'?<select key={p} className="fsel" value={t.params?.[p]||''} onChange={e=>upd(t.id,{params:{...t.params,[p]:e.target.value}})}><option value="">Type</option>{['STRING','INT','BIGINT','DOUBLE','DECIMAL(10,2)','BOOLEAN','TIMESTAMP','DATE'].map(v=><option key={v}>{v}</option>)}</select>:
        p==='joinType'?<select key={p} className="fsel" value={t.params?.[p]||''} onChange={e=>upd(t.id,{params:{...t.params,[p]:e.target.value}})}><option value="">Join</option>{['inner','left','right','full'].map(v=><option key={v} value={v}>{v.toUpperCase()}</option>)}</select>:
        <input key={p} className="finput" placeholder={p} value={t.params?.[p]||''} onChange={e=>upd(t.id,{params:{...t.params,[p]:e.target.value}})}/>
      )}</div></div>);})}</div>
    {xf.length===0&&<div className="empty"><div className="empty-ico">⬢</div><div className="empty-txt">Click a type above to add transforms</div></div>}
  </div></div>);
}

// ─── Sink Forms ───────────────────────────────────────────────────────────────
function SinkForm({config,setConfig,configKey}){
  const sk=config[configKey]||{};const sel=SINK_TYPES.find(s=>s.value===sk.type);
  const upd=(u)=>setConfig(c=>({...c,[configKey]:{...c[configKey],...u}}));
  const setF=(f,v)=>upd({fields:{...sk.fields,[f]:v}});
  return(<div className="fade-in"><div className="fsec"><div className="fsec-title">Select Sink Type</div>
    <div className="tgrid">{SINK_TYPES.map(st=><div key={st.value} className={`tcard ${sk.type===st.value?'sel':''}`} onClick={()=>upd({type:st.value,fields:{}})}>
      <div className="tcard-ico">{st.value==='delta'?'△':st.value==='adls'?'☁':st.value==='azuresql'?'⊞':st.value==='cosmosdb'?'◎':'◇'}</div><div className="tcard-lbl">{st.label}</div></div>)}</div></div>
    {sel&&<div className="fsec fade-in"><div className="fsec-title">Sink Config</div>
      <div className="frow">{sel.fields.map(f=><div className="ffield" key={f}><label className="flbl">{f.replace(/([A-Z])/g,' $1').replace(/^./,s=>s.toUpperCase())} <span className="req">*</span></label>
        {f==='mode'?<select className="fsel" value={sk.fields?.[f]||''} onChange={e=>setF(f,e.target.value)}><option value="">Select</option><option>overwrite</option><option>append</option><option>merge</option></select>:
        <input className="finput" placeholder={f} value={sk.fields?.[f]||''} onChange={e=>setF(f,e.target.value)}/>}</div>)}</div></div>}
  </div>);
}
function ExtendedSinksForm({config,setConfig}){
  const sinks=config.extendedSinks||[];
  const add=()=>setConfig(c=>({...c,extendedSinks:[...(c.extendedSinks||[]),{id:uid(),type:'',fields:{}}]}));
  const upd=(id,u)=>setConfig(c=>({...c,extendedSinks:c.extendedSinks.map(s=>s.id===id?{...s,...u}:s)}));
  const del=(id)=>setConfig(c=>({...c,extendedSinks:c.extendedSinks.filter(s=>s.id!==id)}));
  return(<div className="fade-in"><div className="fsec"><div className="fsec-title">Extended Sinks <span className="badge">{sinks.length}</span></div>
    <div className="sink-cards">{sinks.map((s,idx)=>{const sel=SINK_TYPES.find(x=>x.value===s.type);return(
      <div className="sink-card fade-in" key={s.id}><div className="sink-card-hdr"><div style={{display:'flex',alignItems:'center',gap:7,fontSize:12,fontWeight:600}}><span className="sink-idx">{idx+1}</span>Ext. Sink #{idx+1}</div>
        <button className="btn btn-sm btn-danger" onClick={()=>del(s.id)}>Remove</button></div>
      <div className="frow"><div className="ffield"><label className="flbl">Type</label><select className="fsel" value={s.type} onChange={e=>upd(s.id,{type:e.target.value,fields:{}})}><option value="">Select</option>{SINK_TYPES.map(st=><option key={st.value} value={st.value}>{st.label}</option>)}</select></div></div>
      {sel&&<div className="frow" style={{marginTop:4}}>{sel.fields.map(f=><div className="ffield" key={f}><label className="flbl">{f.replace(/([A-Z])/g,' $1').replace(/^./,s=>s.toUpperCase())}</label>
        {f==='mode'?<select className="fsel" value={s.fields?.[f]||''} onChange={e=>upd(s.id,{fields:{...s.fields,[f]:e.target.value}})}><option value="">Select</option><option>overwrite</option><option>append</option><option>merge</option></select>:
        <input className="finput" placeholder={f} value={s.fields?.[f]||''} onChange={e=>upd(s.id,{fields:{...s.fields,[f]:e.target.value}})}/>}</div>)}</div>}
      </div>);})}</div>
    <div style={{marginTop:10}}><button className="btn" onClick={add}>+ Add Extended Sink</button></div>
    {sinks.length===0&&<div className="empty"><div className="empty-ico">⬡</div><div className="empty-txt">No extended sinks</div></div>}
  </div></div>);
}

// ─── AI Assistant ─────────────────────────────────────────────────────────────
function AIPanel({onApplyConfig}){
  const[messages,setMessages]=useState([{role:'bot',text:"Describe your pipeline in plain English and I'll generate the full config. Include source, DQ rules, transforms, and sinks."}]);
  const[input,setInput]=useState('');const[loading,setLoading]=useState(false);const chatRef=useRef(null);
  useEffect(()=>{chatRef.current?.scrollTo({top:chatRef.current.scrollHeight,behavior:'smooth'});},[messages]);
  const send=async()=>{
    if(!input.trim()||loading)return;const userMsg=input.trim();setInput('');setMessages(m=>[...m,{role:'user',text:userMsg}]);setLoading(true);
    try{const resp=await fetch("https://api.anthropic.com/v1/messages",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({model:"claude-sonnet-4-20250514",max_tokens:1000,
      system:`You are a Spark Scala ETL config assistant. Generate ONLY valid JSON (no markdown/backticks). Schema: {pipelineName,platform:"databricks"|"synapse",source:{type:"adls|delta|azuresql|cosmosdb|db2|vsam",...fields},dqRules:[{column,ruleType:"not_null|unique|range|regex|referential|length|enum",params:{}}],transforms:[{type:"filter|derive|rename|cast|aggregate|join|sql",...params}],sink:{type:"delta|adls|azuresql|cosmosdb|synapse",...fields},extendedSinks:[]}. Fill reasonable defaults.`,
      messages:[{role:"user",content:userMsg}]})});
      const data=await resp.json();const text=data.content?.map(i=>i.text||'').join('')||'';const clean=text.replace(/```json|```/g,'').trim();
      try{const parsed=JSON.parse(clean);setMessages(m=>[...m,{role:'bot',text:'Generated config:',config:parsed,showActions:true}]);}
      catch{setMessages(m=>[...m,{role:'bot',text:`Parsing issue. Try with more specific details.\n\n${clean.substring(0,400)}...`}]);}
    }catch(err){setMessages(m=>[...m,{role:'bot',text:`Error: ${err.message}`}]);}
    setLoading(false);
  };
  const apply=(cfg)=>{
    const mapped={pipelineName:cfg.pipelineName||'',platform:cfg.platform||'databricks',
      source:cfg.source?{type:cfg.source.type,fields:Object.fromEntries(Object.entries(cfg.source).filter(([k])=>k!=='type'))}:{},
      dqRules:(cfg.dqRules||[]).map(r=>({id:uid(),column:r.column,ruleType:r.ruleType,params:r.params||{}})),
      transforms:(cfg.transforms||[]).map(t=>({id:uid(),type:t.type,params:Object.fromEntries(Object.entries(t).filter(([k])=>k!=='type'))})),
      sink:cfg.sink?{type:cfg.sink.type,fields:Object.fromEntries(Object.entries(cfg.sink).filter(([k])=>k!=='type'))}:{},
      extendedSinks:(cfg.extendedSinks||[]).map(s=>({id:uid(),type:s.type,fields:Object.fromEntries(Object.entries(s).filter(([k])=>k!=='type'))}))};
    onApplyConfig(mapped);setMessages(m=>[...m,{role:'bot',text:'✅ Applied! Switch to Builder to review.'}]);
  };
  return(<div className="ai-panel">
    <div className="ai-chat" ref={chatRef}>{messages.map((msg,i)=><div key={i} className={`ai-msg ${msg.role==='user'?'user':'bot'}`}>
      <div>{msg.text}</div>{msg.config&&<div className="ai-config-preview"><pre>{JSON.stringify(msg.config,null,2)}</pre></div>}
      {msg.showActions&&<div className="ai-actions"><button className="btn btn-accent btn-sm" onClick={()=>apply(msg.config)}>✅ Apply</button><button className="btn btn-sm" onClick={()=>navigator.clipboard.writeText(JSON.stringify(msg.config,null,2))}>Copy</button></div>}
    </div>)}{loading&&<div className="ai-msg bot"><div className="ai-typing"><span/><span/><span/></div></div>}</div>
    <div className="ai-examples">{AI_EXAMPLES.map((ex,i)=><div key={i} className="ai-ex" title={ex} onClick={()=>setInput(ex)}>{ex}</div>)}</div>
    <div className="ai-input-area"><textarea className="ai-input" placeholder="Describe your pipeline..." value={input} onChange={e=>setInput(e.target.value)} onKeyDown={e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send();}}} rows={1}/>
      <button className="btn btn-ai" onClick={send} disabled={loading||!input.trim()}>{loading?'...':'⚡ Generate'}</button></div>
  </div>);
}

// ─── Lineage ──────────────────────────────────────────────────────────────────
function LineagePanel({config,columns}){
  const stages=useMemo(()=>{const s=[];
    s.push({label:`Source (${SOURCE_TYPES.find(x=>x.value===config.source?.type)?.label||'N/A'})`,color:'#22d3ee',cols:columns.map(c=>({...c,status:'original'}))});
    if((config.dqRules||[]).length>0)s.push({label:`DQ (${config.dqRules.length} rules)`,color:'#a78bfa',cols:columns.map(c=>({...c,status:config.dqRules.some(r=>r.column===c.name)?'validated':'original'}))});
    let cur=[...columns];
    if((config.transforms||[]).length>0){config.transforms.forEach(t=>{if(t.type==='derive'&&t.params?.columnName)cur=[...cur,{name:t.params.columnName,type:'DERIVED',status:'new'}];if(t.type==='rename'&&t.params?.from&&t.params?.to)cur=cur.map(c=>c.name===t.params.from?{...c,name:t.params.to,status:'renamed'}:c);if(t.type==='cast'&&t.params?.column&&t.params?.targetType)cur=cur.map(c=>c.name===t.params.column?{...c,type:t.params.targetType,status:'cast'}:c);});
      s.push({label:`Transforms (${config.transforms.length})`,color:'#f472b6',cols:cur.map(c=>({...c,status:c.status||'original'}))});}
    s.push({label:`Sink (${SINK_TYPES.find(x=>x.value===config.sink?.type)?.label||'N/A'})`,color:'#34d399',cols:cur.map(c=>({...c,status:c.status||'original'}))});
    (config.extendedSinks||[]).filter(x=>x.type).forEach((es,i)=>s.push({label:`Ext #${i+1} (${SINK_TYPES.find(x=>x.value===es.type)?.label||es.type})`,color:'#fbbf24',cols:cur.map(c=>({...c,status:c.status||'original'}))}));
    return s;},[config,columns]);
  const W=185,colH=16,hdrH=32,gapX=80,pad=20;const maxC=Math.max(...stages.map(s=>s.cols.length));const sH=hdrH+maxC*colH+12;const svgW=stages.length*(W+gapX)+pad;const svgH=sH+pad*2;
  const sc=(s)=>s==='validated'?'#a78bfa':s==='new'?'#34d399':s==='renamed'?'#fbbf24':s==='cast'?'#f472b6':'#94a3b8';
  return(<div className="lineage-panel"><div style={{marginBottom:16}}><div style={{fontSize:14,fontWeight:700,marginBottom:4}}>Column Lineage</div>
    <div style={{fontSize:11,color:'var(--text-secondary)',marginBottom:10}}>Track column flow through pipeline stages</div>
    <div style={{display:'flex',gap:12,fontSize:10}}>{[['Original','#94a3b8'],['Validated','#a78bfa'],['Derived','#34d399'],['Renamed','#fbbf24'],['Cast','#f472b6']].map(([l,c])=><span key={l} style={{display:'flex',alignItems:'center',gap:3}}><span style={{width:7,height:7,borderRadius:'50%',background:c,display:'inline-block'}}/>{l}</span>)}</div></div>
    <div style={{overflowX:'auto'}}><svg width={svgW} height={svgH}>{stages.map((st,si)=>{const x=pad+si*(W+gapX),y=pad;return(<g key={si}>
      <rect x={x} y={y} width={W} height={sH} className="lin-stage" style={{stroke:st.color}}/>
      <rect x={x} y={y} width={W} height={hdrH} rx={10} ry={10} style={{fill:st.color+'20'}}/>
      <text x={x+W/2} y={y+hdrH/2+3} textAnchor="middle" className="lin-label" style={{fill:st.color,fontSize:10}}>{st.label}</text>
      {st.cols.map((col,ci)=>{const cy=y+hdrH+8+ci*colH;return(<g key={ci}><circle cx={x+12} cy={cy+3} r={2.5} fill={sc(col.status)}/><text x={x+20} y={cy+7} className="lin-col">{col.name}</text><text x={x+W-8} y={cy+7} textAnchor="end" className="lin-type">{col.type}</text></g>);})}
      {si<stages.length-1&&<line x1={x+W} y1={y+sH/2} x2={x+W+gapX} y2={y+sH/2} className="lin-line" style={{stroke:st.color+'60'}} strokeDasharray="4 3"/>}
    </g>);})}</svg></div></div>);
}

// ─── Pipeline Catalog ─────────────────────────────────────────────────────────
function CatalogPanel({onEditPipeline,onViewObservability}){
  const[search,setSearch]=useState('');const[filter,setFilter]=useState('all');const[expanded,setExpanded]=useState(null);const[detailTab,setDetailTab]=useState('versions');
  const[showImport,setShowImport]=useState(false);
  const[importData,setImportData]=useState(null); // {headers:[], rows:[], mapping:{}}
  const[importMapping,setImportMapping]=useState({});
  const[importedPipelines,setImportedPipelines]=useState([]); // successfully imported
  const[toast,setToast]=useState(null);
  const showToast=(msg,type='success')=>{setToast({msg,type});setTimeout(()=>setToast(null),3000);};

  const EXPECTED_FIELDS = [
    {key:'pipeline_name',label:'Pipeline Name',required:true},
    {key:'source_type',label:'Source Type',required:true},
    {key:'source_server',label:'Source Server'},
    {key:'source_database',label:'Source Database'},
    {key:'source_schema',label:'Source Schema'},
    {key:'source_table',label:'Source Table'},
    {key:'source_path',label:'Source Path'},
    {key:'source_format',label:'Source Format'},
    {key:'sink_type',label:'Sink Type',required:true},
    {key:'sink_catalog',label:'Sink Catalog'},
    {key:'sink_schema',label:'Sink Schema'},
    {key:'sink_table',label:'Sink Table'},
    {key:'sink_mode',label:'Sink Mode'},
    {key:'sink_merge_keys',label:'Sink Merge Keys'},
    {key:'sink_partition_by',label:'Sink Partition By'},
    {key:'dq_not_null_cols',label:'DQ Not Null Columns'},
    {key:'dq_unique_cols',label:'DQ Unique Columns'},
    {key:'platform',label:'Platform'},
    {key:'schedule',label:'Schedule'},
  ];

  // Auto-map headers to expected fields by fuzzy name match
  const autoMap = (headers) => {
    const mapping = {};
    const normalize = (s) => s.toLowerCase().replace(/[\s_\-./]+/g,'');
    headers.forEach((h,i) => {
      const nh = normalize(h);
      // Try exact-ish matches
      const match = EXPECTED_FIELDS.find(f => {
        const nf = normalize(f.key);
        const nl = normalize(f.label);
        return nh===nf || nh===nl || nf.includes(nh) || nh.includes(nf) ||
          // common excel column name patterns
          (nh.includes('pipeline')&&nh.includes('name')&&f.key==='pipeline_name') ||
          (nh.includes('source')&&nh.includes('type')&&f.key==='source_type') ||
          (nh.includes('sink')&&nh.includes('type')&&f.key==='sink_type') ||
          (nh.includes('server')&&f.key==='source_server') ||
          (nh.includes('database')&&!nh.includes('sink')&&f.key==='source_database') ||
          (nh==='schema'&&f.key==='source_schema') ||
          (nh==='table'&&f.key==='source_table') ||
          (nh.includes('mode')&&f.key==='sink_mode') ||
          (nh.includes('merge')&&f.key==='sink_merge_keys') ||
          (nh.includes('notnull')||nh.includes('not_null'))&&f.key==='dq_not_null_cols';
      });
      if(match) mapping[i] = match.key;
    });
    return mapping;
  };

  const handleFileUpload = (e) => {
    const file = e.target.files?.[0];
    if(!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const text = ev.target.result;
      const sep = text.includes('\t') ? '\t' : ',';
      const lines = text.split('\n').map(l=>l.trim()).filter(Boolean);
      if(lines.length<2) return;
      const headers = lines[0].split(sep).map(h=>h.replace(/^"|"$/g,'').trim());
      const rows = lines.slice(1).map(line => {
        const vals = line.split(sep).map(v=>v.replace(/^"|"$/g,'').trim());
        return headers.map((_,i)=>vals[i]||'');
      });
      const mapping = autoMap(headers);
      setImportData({headers,rows});
      setImportMapping(mapping);
    };
    reader.readAsText(file);
  };

  const getMappedValue = (row, fieldKey) => {
    const colIdx = Object.entries(importMapping).find(([_,v])=>v===fieldKey)?.[0];
    return colIdx !== undefined ? row[parseInt(colIdx)] : '';
  };

  const buildPipelinesFromImport = () => {
    if(!importData) return;
    const pipelines = importData.rows.map((row,i) => {
      const name = getMappedValue(row,'pipeline_name') || `imported_pipeline_${i+1}`;
      const srcType = getMappedValue(row,'source_type') || 'azuresql';
      const snkType = getMappedValue(row,'sink_type') || 'delta';
      const srcFields = {};
      if(getMappedValue(row,'source_server')) srcFields.server=getMappedValue(row,'source_server');
      if(getMappedValue(row,'source_database')) srcFields.database=getMappedValue(row,'source_database');
      if(getMappedValue(row,'source_schema')) srcFields.schema=getMappedValue(row,'source_schema');
      if(getMappedValue(row,'source_table')) srcFields.table=getMappedValue(row,'source_table');
      if(getMappedValue(row,'source_path')) srcFields.path=getMappedValue(row,'source_path');
      if(getMappedValue(row,'source_format')) srcFields.format=getMappedValue(row,'source_format');
      const snkFields = {};
      if(getMappedValue(row,'sink_catalog')) snkFields.catalog=getMappedValue(row,'sink_catalog');
      if(getMappedValue(row,'sink_schema')) snkFields.schema=getMappedValue(row,'sink_schema');
      if(getMappedValue(row,'sink_table')) snkFields.table=getMappedValue(row,'sink_table') || getMappedValue(row,'source_table');
      if(getMappedValue(row,'sink_mode')) snkFields.mode=getMappedValue(row,'sink_mode');
      if(getMappedValue(row,'sink_merge_keys')) snkFields.mergeKeys=getMappedValue(row,'sink_merge_keys');
      if(getMappedValue(row,'sink_partition_by')) snkFields.partitionBy=getMappedValue(row,'sink_partition_by');
      const dqRules = [];
      const nnCols = getMappedValue(row,'dq_not_null_cols');
      if(nnCols) nnCols.split(/[;|,]/).map(c=>c.trim()).filter(Boolean).forEach(c=>dqRules.push({id:uid(),column:c,ruleType:'not_null',params:{}}));
      const uqCols = getMappedValue(row,'dq_unique_cols');
      if(uqCols) uqCols.split(/[;|,]/).map(c=>c.trim()).filter(Boolean).forEach(c=>dqRules.push({id:uid(),column:c,ruleType:'unique',params:{}}));
      return {
        id:'pl-imp-'+uid(),name,owner:'imported',team:'—',
        platform:getMappedValue(row,'platform')||'databricks',status:'active',version:'v1.0',
        lastRun:'—',lastRunStatus:'—',schedule:getMappedValue(row,'schedule')||'Not scheduled',
        tags:['imported'],
        source:{type:srcType.toLowerCase().replace(/\s/g,''),fields:srcFields},
        dqRules,transforms:[],
        sink:{type:snkType.toLowerCase().replace(/\s/g,''),fields:snkFields},
        extendedSinks:[],
        versions:[{ver:'v1.0',date:new Date().toISOString().slice(0,10),author:'import',changes:'Imported from Excel/CSV'}],
        runHistory:[],
      };
    });
    setImportedPipelines(pipelines);
    showToast(`✅ ${pipelines.length} pipelines imported successfully`);
    setShowImport(false);setImportData(null);setImportMapping({});
  };

  const downloadTemplate = () => {
    const headers = 'pipeline_name,source_type,source_server,source_database,source_schema,source_table,sink_type,sink_catalog,sink_schema,sink_table,sink_mode,sink_merge_keys,dq_not_null_cols,dq_unique_cols,platform,schedule';
    const example = 'cdc_customers,azuresql,sqlprod.database.windows.net,customerdb,dbo,customers,delta,gold,customers,dim_customer,merge,customer_id,customer_id;email,customer_id,databricks,Daily 6AM';
    const csv = headers+'\n'+example;
    const blob=new Blob([csv],{type:'text/csv'});const url=URL.createObjectURL(blob);
    const a=document.createElement('a');a.href=url;a.download='pipeline_config_template.csv';a.click();URL.revokeObjectURL(url);
  };

  const allPipelines = [...EXISTING_PIPELINES, ...importedPipelines];
  const filtered=allPipelines.filter(p=>{
    if(filter==='active'&&p.status!=='active')return false;if(filter==='paused'&&p.status!=='paused')return false;if(filter==='failed'&&p.lastRunStatus==='failed'){}else if(filter==='failed'&&p.lastRunStatus!=='failed')return false;
    if(filter==='imported'&&!p.tags?.includes('imported'))return false;
    if(search&&!p.name.includes(search.toLowerCase())&&!p.tags?.some(t=>t.includes(search.toLowerCase())))return false;return true;});

  return(<div className="cat-panel">
    <div style={{marginBottom:16}}><div style={{fontSize:16,fontWeight:700,marginBottom:4}}>Pipeline Catalog</div>
      <div style={{fontSize:11,color:'var(--text-secondary)'}}>Browse, search, and edit existing pipeline configurations</div></div>
    <div className="cat-search"><input className="finput" placeholder="Search pipelines by name or tag..." value={search} onChange={e=>setSearch(e.target.value)} style={{fontFamily:'DM Sans',fontSize:12}}/>
      <button className="btn btn-sm" style={{borderColor:'var(--accent-amber)',color:'var(--accent-amber)'}} onClick={()=>setShowImport(true)}>📥 Import Excel</button>
      <button className="btn btn-primary btn-sm" onClick={()=>onEditPipeline(null)}>+ New Pipeline</button></div>
    <div className="cat-filters">
      {[['all','All'],['active','Active'],['paused','Paused'],['failed','Failed'],['imported','Imported']].map(([v,l])=>
        <div key={v} className={`cat-filter ${filter===v?'active':''}`} onClick={()=>setFilter(v)}>{l}
          {v==='imported'&&importedPipelines.length>0&&<span style={{marginLeft:3,opacity:.7}}>({importedPipelines.length})</span>}
        </div>)}
    </div>

    {/* Imported success banner */}
    {importedPipelines.length>0&&filter!=='imported'&&<div style={{padding:'10px 14px',background:'rgba(52,211,153,.06)',border:'1px solid rgba(52,211,153,.15)',borderRadius:'var(--radius)',marginBottom:12,display:'flex',alignItems:'center',justifyContent:'space-between',fontSize:11}}>
      <span style={{color:'var(--accent-green)'}}>✅ {importedPipelines.length} pipelines imported — click "Imported" filter to view, or click ✎ Edit to configure each one</span>
      <button className="btn btn-xs" onClick={()=>setFilter('imported')}>View Imported</button>
    </div>}

    {filtered.map(pl=>(
      <div className="pl-card" key={pl.id}>
        <div className="pl-card-main" onClick={()=>setExpanded(expanded===pl.id?null:pl.id)}>
          <div className="pl-card-info">
            <div className="pl-card-name">{pl.name}</div>
            <div className="pl-card-meta">
              <span>👤 {pl.owner}</span><span>🏷 {pl.team}</span><span>📋 {pl.version}</span><span>🕐 {pl.schedule}</span>
              {pl.lastRunStatus!=='—'&&<span className={`pl-run-status ${pl.lastRunStatus}`}>● {pl.lastRunStatus}</span>}
            </div>
            <div className="pl-card-tags">{pl.tags?.map(t=><span className="tag" key={t}>{t}</span>)}</div>
          </div>
          <div className="pl-card-actions" onClick={e=>e.stopPropagation()}>
            <span className={`pl-status ${pl.status}`}>{pl.status}</span>
            <button className="btn btn-sm btn-primary" onClick={()=>onEditPipeline(pl)}>✎ Edit</button>
            {pl.runHistory?.length>0&&<button className="btn btn-sm" onClick={()=>onViewObservability(pl)}>📊 Monitor</button>}
          </div>
        </div>
        {expanded===pl.id&&<div className="pl-detail">
          <div className="pl-detail-tabs">
            {['versions','runs','config'].map(t=><div key={t} className={`pl-dtab ${detailTab===t?'active':''}`} onClick={()=>setDetailTab(t)}>{t==='versions'?'Version History':t==='runs'?'Run History':'Current Config'}</div>)}
          </div>
          {detailTab==='versions'&&<div className="ver-list">{(pl.versions||[]).map(v=>(
            <div className="ver-item" key={v.ver}><div><span className="ver-badge">{v.ver}</span></div>
              <div style={{flex:1}}><div style={{fontSize:10,color:'var(--text-muted)',marginBottom:4}}>{v.date} by <strong style={{color:'var(--text-primary)'}}>{v.author}</strong></div>
                <div className="ver-changes">{v.changes.split('\n').map((l,i)=><div key={i} className={l.startsWith('+')?'add':l.startsWith('-')?'rem':''}>{l}</div>)}</div>
              </div>
              <button className="btn btn-sm" onClick={()=>onEditPipeline(pl)} title="Restore this version">↻</button>
            </div>))}</div>}
          {detailTab==='runs'&&(pl.runHistory?.length>0?<div style={{overflowX:'auto'}}><table className="run-tbl"><thead><tr><th>Date</th><th>Status</th><th>Duration</th><th>Read</th><th>Written</th><th>DQ Pass</th><th>DQ Fail</th></tr></thead>
            <tbody>{pl.runHistory.map(r=><tr key={r.id}><td>{r.date}</td><td><span className={`pl-run-status ${r.status}`}>{r.status}</span></td><td>{r.duration}</td><td>{r.rowsRead.toLocaleString()}</td><td>{r.rowsWritten.toLocaleString()}</td><td style={{color:'var(--accent-green)'}}>{r.dqPass.toLocaleString()}</td><td style={{color:r.dqFail>0?'var(--accent-red)':'var(--text-muted)'}}>{r.dqFail.toLocaleString()}</td></tr>)}</tbody></table></div>
            :<div style={{fontSize:11,color:'var(--text-muted)',padding:12}}>No runs yet — edit and deploy this pipeline to start</div>)}
          {detailTab==='config'&&<div style={{background:'var(--bg-input)',borderRadius:8,padding:12,maxHeight:300,overflowY:'auto'}}><pre style={{fontFamily:'JetBrains Mono',fontSize:10,color:'var(--text-secondary)'}}>{JSON.stringify({source:{type:pl.source.type,...pl.source.fields},dqRules:pl.dqRules?.length||0,transforms:pl.transforms?.length||0,sink:{type:pl.sink.type,...pl.sink.fields},extendedSinks:pl.extendedSinks?.length||0},null,2)}</pre></div>}
        </div>}
      </div>
    ))}
    {filtered.length===0&&<div className="empty"><div className="empty-ico">🔍</div><div className="empty-txt">No pipelines match your search</div></div>}

    {/* ── Import Modal ── */}
    {showImport&&<div className="modal-ov" onClick={()=>{setShowImport(false);setImportData(null);}}>
      <div className="modal" style={{width:760,maxWidth:'95vw'}} onClick={e=>e.stopPropagation()}>
        <div style={{fontSize:17,fontWeight:700,marginBottom:3}}>Import Pipelines from Excel / CSV</div>
        <div style={{fontSize:12,color:'var(--text-secondary)',marginBottom:18}}>Upload your config spreadsheet — columns are auto-mapped to pipeline fields</div>

        {!importData ? <>
          {/* Upload zone */}
          <div className="import-upload" onClick={()=>document.getElementById('import-file-input').click()}>
            <div className="import-upload-ico">📁</div>
            <div className="import-upload-title">Click to upload CSV or Excel export</div>
            <div className="import-upload-hint">Accepts .csv, .tsv — one pipeline per row</div>
            <input id="import-file-input" type="file" accept=".csv,.tsv,.txt,.xls,.xlsx" style={{display:'none'}} onChange={handleFileUpload}/>
          </div>
          <div style={{marginTop:14,display:'flex',gap:8,alignItems:'center'}}>
            <button className="btn btn-sm" onClick={downloadTemplate}>⬇ Download Template CSV</button>
            <span style={{fontSize:10,color:'var(--text-muted)'}}>Pre-filled with expected column headers and one example row</span>
          </div>
          <div style={{marginTop:18,padding:14,background:'var(--bg-card)',border:'1px solid var(--border)',borderRadius:'var(--radius)',fontSize:11,color:'var(--text-secondary)'}}>
            <div style={{fontWeight:600,marginBottom:6,color:'var(--text-primary)'}}>Expected columns (all optional — we auto-map what we find)</div>
            <div style={{display:'flex',flexWrap:'wrap',gap:'4px 14px',fontFamily:'JetBrains Mono',fontSize:10}}>
              {EXPECTED_FIELDS.map(f=><span key={f.key} style={f.required?{color:'var(--accent-cyan)'}:{}}>{f.key}{f.required?' *':''}</span>)}
            </div>
          </div>
        </> : <>
          {/* Preview & mapping */}
          <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:10}}>
            <div style={{fontSize:13,fontWeight:600}}>{importData.rows.length} rows detected</div>
            <button className="btn btn-sm btn-danger" onClick={()=>{setImportData(null);setImportMapping({});}}>✕ Clear & Re-upload</button>
          </div>

          {/* Column mapping */}
          <div style={{marginBottom:14}}>
            <div style={{fontSize:11,fontWeight:600,color:'var(--text-muted)',marginBottom:6}}>COLUMN MAPPING (auto-detected — adjust if needed)</div>
            <div className="import-map">{importData.headers.map((h,i)=>(
              <div className="import-map-item" key={i}>
                <span style={{fontFamily:'JetBrains Mono',fontSize:10,color:'var(--text-primary)'}}>{h}</span>
                <span className="map-arrow">→</span>
                <select className="fsel" value={importMapping[i]||''} onChange={e=>{const v=e.target.value;setImportMapping(m=>({...m,[i]:v||undefined}));}} style={{width:'auto',minWidth:120,fontSize:10,padding:'3px 6px'}}>
                  <option value="">— skip —</option>
                  {EXPECTED_FIELDS.map(f=><option key={f.key} value={f.key}>{f.label}</option>)}
                </select>
              </div>
            ))}</div>
          </div>

          {/* Data preview */}
          <div className="import-preview">
            <table className="import-tbl">
              <thead><tr><th className="row-num">#</th>{importData.headers.map((h,i)=>(
                <th key={i} style={importMapping[i]?{color:'var(--accent-green)'}:{}}>{importMapping[i]?EXPECTED_FIELDS.find(f=>f.key===importMapping[i])?.label:h}<br/>
                  {importMapping[i]&&<span style={{fontSize:8,opacity:.6,fontWeight:400}}>← {h}</span>}
                  {!importMapping[i]&&<span style={{fontSize:8,opacity:.4,fontWeight:400}}>skipped</span>}
                </th>
              ))}</tr></thead>
              <tbody>{importData.rows.slice(0,20).map((row,ri)=>(
                <tr key={ri}><td className="row-num">{ri+1}</td>
                  {row.map((cell,ci)=><td key={ci} className={importMapping[ci]?'mapped':'unmapped'}>{cell||'—'}</td>)}
                </tr>
              ))}</tbody>
            </table>
            {importData.rows.length>20&&<div style={{padding:8,textAlign:'center',fontSize:10,color:'var(--text-muted)'}}>...and {importData.rows.length-20} more rows</div>}
          </div>

          {/* Summary */}
          <div className="import-summary">
            <div className="import-stat"><div className="import-stat-val" style={{color:'var(--accent-cyan)'}}>{importData.rows.length}</div><div className="import-stat-lbl">Pipelines</div></div>
            <div className="import-stat"><div className="import-stat-val" style={{color:'var(--accent-green)'}}>{Object.values(importMapping).filter(Boolean).length}</div><div className="import-stat-lbl">Mapped Cols</div></div>
            <div className="import-stat"><div className="import-stat-val" style={{color:'var(--text-muted)'}}>{importData.headers.length - Object.values(importMapping).filter(Boolean).length}</div><div className="import-stat-lbl">Skipped Cols</div></div>
          </div>

          <div className="import-actions">
            <button className="btn btn-sm" onClick={()=>{setShowImport(false);setImportData(null);}}>Cancel</button>
            <button className="btn btn-primary" onClick={buildPipelinesFromImport} disabled={!Object.values(importMapping).includes('pipeline_name')&&!Object.values(importMapping).includes('source_table')}>
              📥 Import {importData.rows.length} Pipelines
            </button>
          </div>
        </>}
      </div>
    </div>}

    {toast&&<div className={`toast ${toast.type}`}>{toast.msg}</div>}
  </div>);
}

// ─── Data Observability ───────────────────────────────────────────────────────
function ObservabilityPanel({selectedPipeline}){
  const allRuns=EXISTING_PIPELINES.flatMap(p=>p.runHistory.map(r=>({...r,pipeline:p.name,pipelineStatus:p.status})));
  const pl=selectedPipeline||EXISTING_PIPELINES[0];
  const totalRuns=allRuns.length;const successRuns=allRuns.filter(r=>r.status==='success').length;
  const totalRows=allRuns.reduce((a,r)=>a+r.rowsRead,0);const totalDqFail=allRuns.reduce((a,r)=>a+r.dqFail,0);
  const dqScore=totalRows>0?((totalRows-totalDqFail)/totalRows*100).toFixed(2):100;

  return(<div className="obs-panel">
    <div style={{marginBottom:16}}><div style={{fontSize:16,fontWeight:700,marginBottom:4}}>Data Observability</div>
      <div style={{fontSize:11,color:'var(--text-secondary)'}}>Real-time monitoring, data quality scores, SLA tracking & alerts</div></div>

    {/* KPI Cards */}
    <div className="obs-grid">
      <div className="obs-metric"><div className="obs-metric-label">Total Runs (7d)</div><div className="obs-metric-value" style={{color:'var(--accent-cyan)'}}>{totalRuns}</div><div className="obs-metric-delta up">↑ 12% vs last week</div></div>
      <div className="obs-metric"><div className="obs-metric-label">Success Rate</div><div className="obs-metric-value" style={{color:'var(--accent-green)'}}>{(successRuns/totalRuns*100).toFixed(1)}%</div><div className="obs-metric-sub">{successRuns}/{totalRuns} runs passed</div></div>
      <div className="obs-metric"><div className="obs-metric-label">DQ Score</div><div className="obs-metric-value" style={{color:parseFloat(dqScore)>99?'var(--accent-green)':'var(--accent-amber)'}}>{dqScore}%</div><div className="obs-metric-sub">{totalDqFail.toLocaleString()} rows failed / {totalRows.toLocaleString()} total</div></div>
      <div className="obs-metric"><div className="obs-metric-label">Rows Processed (7d)</div><div className="obs-metric-value" style={{color:'var(--accent-purple)'}}>{(totalRows/1000).toFixed(0)}K</div><div className="obs-metric-delta up">↑ 3.2% growth</div></div>
      <div className="obs-metric"><div className="obs-metric-label">Avg Duration</div><div className="obs-metric-value" style={{color:'var(--text-primary)'}}>5m 42s</div><div className="obs-metric-delta down">↑ 8s slower</div></div>
      <div className="obs-metric"><div className="obs-metric-label">Active Pipelines</div><div className="obs-metric-value" style={{color:'var(--accent-cyan)'}}>{EXISTING_PIPELINES.filter(p=>p.status==='active').length}</div><div className="obs-metric-sub">{EXISTING_PIPELINES.length} total</div></div>
    </div>

    {/* Run Volume Chart */}
    <div className="obs-chart">
      <div className="obs-chart-title"><span>Run Volume & Status (Last 7 Days)</span><span style={{fontSize:10,color:'var(--text-muted)'}}>All pipelines</span></div>
      <div className="bar-chart">
        {[{day:'Mar 22',s:3,f:0,w:0},{day:'Mar 23',s:4,f:0,w:0},{day:'Mar 24',s:3,f:1,w:0},{day:'Mar 25',s:4,f:1,w:0},{day:'Mar 26',s:3,f:0,w:1},{day:'Mar 27',s:4,f:0,w:0},{day:'Mar 28',s:4,f:0,w:0}].map(d=>
          <div className="bar-col" key={d.day}>
            <div className="bar-val">{d.s+d.f+d.w}</div>
            <div style={{display:'flex',flexDirection:'column',width:'100%',gap:1,flex:1,justifyContent:'flex-end'}}>
              {d.f>0&&<div className="bar-rect" style={{height:`${d.f/(d.s+d.f+d.w)*100}%`,background:'var(--accent-red)'}}/>}
              {d.w>0&&<div className="bar-rect" style={{height:`${d.w/(d.s+d.f+d.w)*100}%`,background:'var(--accent-amber)'}}/>}
              <div className="bar-rect" style={{height:`${d.s/(d.s+d.f+d.w)*100}%`,background:'var(--accent-green)'}}/>
            </div>
            <div className="bar-label">{d.day.split(' ')[1]}</div>
          </div>
        )}
      </div>
      <div style={{display:'flex',gap:14,justifyContent:'center',marginTop:10,fontSize:9}}>
        <span style={{display:'flex',alignItems:'center',gap:3}}><span style={{width:8,height:8,borderRadius:2,background:'var(--accent-green)',display:'inline-block'}}/>Success</span>
        <span style={{display:'flex',alignItems:'center',gap:3}}><span style={{width:8,height:8,borderRadius:2,background:'var(--accent-amber)',display:'inline-block'}}/>Warning</span>
        <span style={{display:'flex',alignItems:'center',gap:3}}><span style={{width:8,height:8,borderRadius:2,background:'var(--accent-red)',display:'inline-block'}}/>Failed</span>
      </div>
    </div>

    {/* DQ Scores per Pipeline */}
    <div className="obs-chart">
      <div className="obs-chart-title"><span>Data Quality Score by Pipeline</span></div>
      <div className="bar-chart" style={{height:100}}>
        {EXISTING_PIPELINES.map(p=>{const total=p.runHistory.reduce((a,r)=>a+r.rowsRead,0);const fail=p.runHistory.reduce((a,r)=>a+r.dqFail,0);const pct=total>0?((total-fail)/total*100):100;
          return(<div className="bar-col" key={p.id}><div className="bar-val">{pct.toFixed(1)}%</div>
            <div style={{width:'100%',flex:1,display:'flex',alignItems:'flex-end'}}><div className="bar-rect" style={{height:`${pct}%`,background:pct>99.9?'var(--accent-green)':pct>99?'var(--accent-amber)':'var(--accent-red)',width:'100%'}}/></div>
            <div className="bar-label" style={{maxWidth:60,overflow:'hidden',textOverflow:'ellipsis'}}>{p.name.replace(/_/g,' ').substring(0,12)}</div></div>);
        })}
      </div>
    </div>

    {/* SLA Tracking */}
    <div className="obs-chart">
      <div className="obs-chart-title"><span>SLA Compliance (30 Day)</span></div>
      {[{name:'cdc_customer_load',sla:99.5,actual:99.8},{name:'mainframe_ingest_orders',sla:99.0,actual:98.2},{name:'sales_fanout_weekly',sla:95.0,actual:75.0},{name:'cosmos_product_sync',sla:99.0,actual:100}].map(s=>
        <div className="sla-bar" key={s.name}><div className="sla-name">{s.name}</div>
          <div style={{flex:1}}>
            <div className="sla-track"><div className="sla-fill" style={{width:`${s.actual}%`,background:s.actual>=s.sla?'var(--accent-green)':s.actual>=s.sla*0.95?'var(--accent-amber)':'var(--accent-red)'}}/></div>
            <div style={{display:'flex',justifyContent:'space-between',fontSize:8,color:'var(--text-muted)',marginTop:2}}><span>SLA: {s.sla}%</span><span>{s.actual>=s.sla?'✓ Met':'✕ Breached'}</span></div>
          </div>
          <div className="sla-pct" style={{color:s.actual>=s.sla?'var(--accent-green)':'var(--accent-red)'}}>{s.actual}%</div>
        </div>
      )}
    </div>

    {/* Alerts */}
    <div className="obs-chart">
      <div className="obs-chart-title"><span>Active Alerts & Incidents</span><span style={{fontSize:10}}><span className="badge" style={{background:'var(--accent-red)'}}>2</span></span></div>
      <div className="alert-list">
        <div className="alert-item critical"><div className="alert-ico">🔴</div><div className="alert-body"><div className="alert-title">SLA Breach: sales_fanout_weekly</div><div>Uptime dropped to 75% (SLA: 95%). Pipeline paused after consecutive failures. Synapse pool was in paused state.</div><div className="alert-time">Mar 21, 2026 · 9:45 AM · Unresolved</div></div></div>
        <div className="alert-item warning"><div className="alert-ico">🟡</div><div className="alert-body"><div className="alert-title">DQ Anomaly: mainframe_ingest_orders</div><div>300 DQ failures detected on Mar 26 (0.03% fail rate). Normally &lt;5 failures/run. Possible upstream data issue.</div><div className="alert-time">Mar 26, 2026 · 7:22 AM · Under review</div></div></div>
        <div className="alert-item info"><div className="alert-ico">🔵</div><div className="alert-body"><div className="alert-title">Schema Drift Detected: cdc_customer_load</div><div>New column `loyalty_tier` (STRING) detected in source but not in pipeline config. Consider adding to transforms.</div><div className="alert-time">Mar 27, 2026 · 6:20 AM · Informational</div></div></div>
        <div className="alert-item resolved"><div className="alert-ico">🟢</div><div className="alert-body"><div className="alert-title">Resolved: JDBC timeout on cdc_customer_load</div><div>Connection timeout on Mar 25 was caused by SQL server maintenance window. Auto-retried successfully on Mar 26.</div><div className="alert-time">Mar 25, 2026 · 6:15 AM · Auto-resolved</div></div></div>
      </div>
    </div>

    {/* Pipeline-level run detail */}
    <div className="obs-chart">
      <div className="obs-chart-title"><span>Pipeline Run Detail: {pl.name}</span>
        <select className="fsel" style={{width:200,fontSize:10,padding:'4px 8px'}} defaultValue={pl.id}>
          {EXISTING_PIPELINES.map(p=><option key={p.id} value={p.id}>{p.name}</option>)}
        </select>
      </div>
      <div style={{overflowX:'auto'}}><table className="run-tbl"><thead><tr><th>Timestamp</th><th>Status</th><th>Duration</th><th>Rows In</th><th>Rows Out</th><th>DQ Pass</th><th>DQ Fail</th><th>Error</th></tr></thead>
        <tbody>{pl.runHistory.map(r=><tr key={r.id}><td>{r.date}</td><td><span className={`pl-run-status ${r.status}`}>● {r.status}</span></td><td>{r.duration}</td><td>{r.rowsRead.toLocaleString()}</td><td>{r.rowsWritten.toLocaleString()}</td><td style={{color:'var(--accent-green)'}}>{r.dqPass.toLocaleString()}</td><td style={{color:r.dqFail>0?'var(--accent-red)':'var(--text-muted)'}}>{r.dqFail}</td><td style={{color:'var(--accent-red)',maxWidth:200,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{r.error||'—'}</td></tr>)}</tbody></table></div>
    </div>
  </div>);
}

// ─── Approval Panel ───────────────────────────────────────────────────────────
function ApprovalPanel(){
  const[requests,setRequests]=useState([
    {id:1,pipeline:'cdc_customer_load',status:'pending',author:'mrutyumjaya',submitted:'2026-03-27 14:30',version:'v1.2 → v1.3',changes:'+Added range check on balance\n+Added derive: full_name\n-Removed filter on region_code'},
    {id:2,pipeline:'mainframe_ingest_orders',status:'approved',author:'data_team',submitted:'2026-03-25 09:15',version:'v2.0 → v2.1',changes:'+Changed sink mode to merge\n+Added mergeKeys: order_id'},
    {id:3,pipeline:'sales_fanout_weekly',status:'rejected',author:'analyst_a',submitted:'2026-03-24 16:45',version:'v1.0 → v1.1',changes:'+Added extended sink: Cosmos DB\n-Removed all DQ rules',rejectReason:'Missing DQ rules. Add not_null on txn_id.'},
  ]);
  return(<div style={{padding:20,overflowY:'auto',height:'100%'}}>
    <div style={{marginBottom:16}}><div style={{fontSize:16,fontWeight:700,marginBottom:4}}>Pipeline Approvals</div><div style={{fontSize:11,color:'var(--text-secondary)'}}>Review config changes before production</div></div>
    {requests.map(req=><div className="appr-card" key={req.id}>
      <div className="appr-hdr"><div><div style={{fontFamily:'JetBrains Mono',fontSize:12,fontWeight:600}}>{req.pipeline}</div><div style={{fontSize:10,color:'var(--text-muted)'}}>{req.version} · by {req.author} · {req.submitted}</div></div>
        <span className={`appr-status ${req.status}`}>{req.status==='pending'?'⏳ Pending':req.status==='approved'?'✓ Approved':'✕ Rejected'}</span></div>
      <div className="appr-diff">{req.changes.split('\n').map((l,i)=><div key={i} className={l.startsWith('+')?'add':l.startsWith('-')?'rem':''}>{l}</div>)}</div>
      {req.rejectReason&&<div style={{fontSize:11,color:'var(--accent-red)',padding:'6px 10px',background:'rgba(239,68,68,.06)',borderRadius:5,marginBottom:8}}>💬 {req.rejectReason}</div>}
      {req.status==='pending'&&<div style={{display:'flex',gap:6}}><button className="btn btn-approve btn-sm" onClick={()=>setRequests(r=>r.map(x=>x.id===req.id?{...x,status:'approved'}:x))}>✓ Approve</button><button className="btn btn-danger btn-sm" onClick={()=>setRequests(r=>r.map(x=>x.id===req.id?{...x,status:'rejected',rejectReason:'Needs revision.'}:x))}>✕ Reject</button></div>}
    </div>)}
    <div style={{marginTop:20}}><div style={{fontSize:13,fontWeight:600,marginBottom:12}}>Activity Timeline</div>
      <div className="timeline">
        <div className="tl-item"><div className="tl-dot amber">⏳</div><div className="tl-content"><strong>mrutyumjaya</strong> submitted v1.3 of <span style={{fontFamily:'JetBrains Mono',fontSize:10}}>cdc_customer_load</span><div className="tl-time">Mar 27 · 2:30 PM</div></div></div>
        <div className="tl-item"><div className="tl-dot green">✓</div><div className="tl-content"><strong>lead_engineer</strong> approved v2.1 of <span style={{fontFamily:'JetBrains Mono',fontSize:10}}>mainframe_ingest_orders</span><div className="tl-time">Mar 25 · 11:00 AM</div></div></div>
        <div className="tl-item"><div className="tl-dot blue">✕</div><div className="tl-content"><strong>lead_engineer</strong> rejected v1.1 of <span style={{fontFamily:'JetBrains Mono',fontSize:10}}>sales_fanout_weekly</span><div className="tl-time">Mar 24 · 5:30 PM</div></div></div>
      </div>
    </div>
  </div>);
}

// ─── Data Discovery & Catalog ─────────────────────────────────────────────────
function DiscoveryPanel({onNavigateToPipeline}){
  // Build dataset catalog from existing pipeline configs
  const datasets = useMemo(()=>{
    const map = new Map();
    const PII_COLUMNS = ['email','phone','ssn','social_security','first_name','last_name','address','dob','date_of_birth','salary','bank_account','credit_card'];
    const COL_DESCRIPTIONS = {
      customer_id:'Unique customer identifier',first_name:'Customer first name',last_name:'Customer surname',
      email:'Contact email address',phone:'Phone number',created_date:'Record creation timestamp',
      status:'Current record status code',balance:'Account balance amount',region_code:'Geographic region identifier',
      is_active:'Active/inactive flag',full_name:'Derived: first + last name',etl_load_ts:'ETL load timestamp',row_hash:'Row integrity hash',
    };
    EXISTING_PIPELINES.forEach(pl=>{
      // Source dataset
      const srcKey = `${pl.source.type}:${pl.source.fields?.schema||''}.${pl.source.fields?.table||pl.source.fields?.container||pl.source.fields?.dataset||'unknown'}`;
      if(!map.has(srcKey)){
        const tier = pl.source.type==='vsam'||pl.source.type==='db2'?'bronze':pl.source.type==='adls'?'bronze':'silver';
        map.set(srcKey,{
          id:srcKey,name:pl.source.fields?.table||pl.source.fields?.container||pl.source.fields?.dataset||'unknown',
          schema:pl.source.fields?.schema||pl.source.fields?.database||'—',
          fullPath:`${pl.source.fields?.database||pl.source.fields?.endpoint||''}.${pl.source.fields?.schema||''}.${pl.source.fields?.table||''}`.replace(/^\.+|\.+$/g,''),
          type:pl.source.type,tier,
          platform:SOURCE_TYPES.find(s=>s.value===pl.source.type)?.label||pl.source.type,
          columns:SAMPLE_COLUMNS.map(c=>({
            ...c,
            isPII:PII_COLUMNS.some(p=>c.name.toLowerCase().includes(p)),
            description:COL_DESCRIPTIONS[c.name]||'',
            dqRules:pl.dqRules?.filter(r=>r.column===c.name).map(r=>{const rt=DQ_RULE_TYPES.find(x=>x.value===r.ruleType);return rt?.label||r.ruleType;})||[],
            nullPct:c.name.includes('id')?0:c.name==='phone'?12.3:c.name==='email'?0.5:c.type==='BOOLEAN'?0:Math.round(Math.random()*8*10)/10,
            distinct:c.name.includes('id')?125430:c.name==='status'?4:c.name==='region_code'?12:c.name==='is_active'?2:c.type==='TIMESTAMP'?124500:Math.floor(Math.random()*50000),
          })),
          pipelines:[{id:pl.id,name:pl.name,direction:'read',owner:pl.owner}],
          owner:pl.owner,team:pl.team,
          rowCount:pl.runHistory?.[0]?.rowsRead||0,
          lastUpdated:pl.lastRun,
          description:`Source table for ${pl.name}`,
        });
      } else {
        const ds=map.get(srcKey);
        if(!ds.pipelines.find(p=>p.id===pl.id))ds.pipelines.push({id:pl.id,name:pl.name,direction:'read',owner:pl.owner});
      }
      // Sink dataset
      const snkKey = `${pl.sink.type}:${pl.sink.fields?.schema||pl.sink.fields?.catalog||''}.${pl.sink.fields?.table||pl.sink.fields?.container||'unknown'}`;
      if(!map.has(snkKey)){
        const tier = (pl.sink.fields?.catalog||'').includes('gold')?'gold':(pl.sink.fields?.catalog||'').includes('silver')?'silver':'silver';
        const derivedCols = pl.transforms?.filter(t=>t.type==='derive').map(t=>({
          name:t.params?.columnName||'derived_col',type:'DERIVED',
          isPII:false,description:`Derived: ${t.params?.expression||''}`,dqRules:[],nullPct:0,distinct:0,
        }))||[];
        map.set(snkKey,{
          id:snkKey,name:pl.sink.fields?.table||pl.sink.fields?.container||'unknown',
          schema:pl.sink.fields?.schema||pl.sink.fields?.catalog||'—',
          fullPath:`${pl.sink.fields?.catalog||''}.${pl.sink.fields?.schema||''}.${pl.sink.fields?.table||''}`.replace(/^\.+|\.+$/g,''),
          type:pl.sink.type,tier,
          platform:SINK_TYPES.find(s=>s.value===pl.sink.type)?.label||pl.sink.type,
          columns:[...SAMPLE_COLUMNS.map(c=>({
            ...c,
            isPII:PII_COLUMNS.some(p=>c.name.toLowerCase().includes(p)),
            description:COL_DESCRIPTIONS[c.name]||'',
            dqRules:pl.dqRules?.filter(r=>r.column===c.name).map(r=>{const rt=DQ_RULE_TYPES.find(x=>x.value===r.ruleType);return rt?.label||r.ruleType;})||[],
            nullPct:0,distinct:c.name.includes('id')?125430:c.name==='status'?4:c.name==='region_code'?12:c.name==='is_active'?2:Math.floor(Math.random()*50000),
          })),...derivedCols],
          pipelines:[{id:pl.id,name:pl.name,direction:'write',owner:pl.owner}],
          owner:pl.owner,team:pl.team,
          rowCount:pl.runHistory?.[0]?.rowsWritten||0,
          lastUpdated:pl.lastRun,
          description:`Sink table for ${pl.name}. Mode: ${pl.sink.fields?.mode||'—'}`,
        });
      } else {
        const ds=map.get(snkKey);
        if(!ds.pipelines.find(p=>p.id===pl.id))ds.pipelines.push({id:pl.id,name:pl.name,direction:'write',owner:pl.owner});
      }
    });
    return Array.from(map.values());
  },[]);

  const[search,setSearch]=useState('');
  const[selectedDs,setSelectedDs]=useState(datasets[0]||null);
  const[detailTab,setDetailTab]=useState('columns');
  const[tierFilter,setTierFilter]=useState('all');

  // Group by tier
  const grouped = useMemo(()=>{
    const g={bronze:[],silver:[],gold:[]};
    datasets.filter(d=>{
      if(tierFilter!=='all'&&d.tier!==tierFilter)return false;
      if(search&&!d.name.toLowerCase().includes(search.toLowerCase())&&!d.fullPath.toLowerCase().includes(search.toLowerCase())&&!d.schema.toLowerCase().includes(search.toLowerCase()))return false;
      return true;
    }).forEach(d=>g[d.tier]?.push(d));
    return g;
  },[datasets,search,tierFilter]);

  const totalCols = datasets.reduce((a,d)=>a+d.columns.length,0);
  const piiCols = datasets.reduce((a,d)=>a+d.columns.filter(c=>c.isPII).length,0);
  const dqCovered = datasets.reduce((a,d)=>a+d.columns.filter(c=>c.dqRules.length>0).length,0);

  return(<div className="disc-panel">
    <div style={{marginBottom:14}}><div style={{fontSize:16,fontWeight:700,marginBottom:3}}>Data Discovery & Catalog</div>
      <div style={{fontSize:11,color:'var(--text-secondary)'}}>Auto-generated from pipeline configs — browse datasets, columns, PII, lineage</div></div>

    {/* Summary stats */}
    <div className="disc-stats">
      <div className="disc-stat"><div className="disc-stat-val" style={{color:'var(--accent-cyan)'}}>{datasets.length}</div><div className="disc-stat-lbl">Datasets</div></div>
      <div className="disc-stat"><div className="disc-stat-val" style={{color:'var(--accent-purple)'}}>{totalCols}</div><div className="disc-stat-lbl">Total Columns</div></div>
      <div className="disc-stat"><div className="disc-stat-val" style={{color:'var(--accent-pink)'}}>{piiCols}</div><div className="disc-stat-lbl">PII Columns</div></div>
      <div className="disc-stat"><div className="disc-stat-val" style={{color:'var(--accent-green)'}}>{dqCovered}</div><div className="disc-stat-lbl">DQ Covered</div></div>
      <div className="disc-stat"><div className="disc-stat-val" style={{color:'var(--accent-amber)'}}>{EXISTING_PIPELINES.length}</div><div className="disc-stat-lbl">Pipelines</div></div>
    </div>

    {/* Search + filter */}
    <div className="disc-search">
      <input className="finput" placeholder="Search tables, schemas, columns..." value={search} onChange={e=>setSearch(e.target.value)} style={{fontFamily:'DM Sans',fontSize:12}}/>
      <div style={{display:'flex',gap:4}}>
        {['all','bronze','silver','gold'].map(t=>
          <div key={t} className={`cat-filter ${tierFilter===t?'active':''}`} onClick={()=>setTierFilter(t)} style={{fontSize:10}}>
            {t==='all'?'All':t.charAt(0).toUpperCase()+t.slice(1)}
          </div>
        )}
      </div>
    </div>

    {/* Main layout: sidebar tree + detail panel */}
    <div className="disc-layout">
      {/* Dataset Tree */}
      <div className="disc-sidebar">
        <div className="disc-sidebar-hdr"><span>Datasets ({datasets.length})</span></div>
        <div className="disc-tree">
          {['bronze','silver','gold'].map(tier=>{
            const items=grouped[tier]||[];
            if(items.length===0) return null;
            return(<div className="disc-tree-group" key={tier}>
              <div className="disc-tree-group-hdr">
                <span className={`disc-tier ${tier}`}>{tier}</span>
                <span style={{fontSize:9,color:'var(--text-muted)'}}>{items.length}</span>
              </div>
              {items.map(ds=>(
                <div key={ds.id} className={`disc-tree-item ${selectedDs?.id===ds.id?'active':''}`} onClick={()=>{setSelectedDs(ds);setDetailTab('columns');}}>
                  <span style={{fontSize:12,opacity:.6}}>{ds.type==='delta'?'△':ds.type==='azuresql'?'⊞':ds.type==='cosmosdb'?'◎':ds.type==='db2'?'⬡':ds.type==='adls'?'☁':'▤'}</span>
                  <span style={{flex:1,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{ds.name}</span>
                  {ds.columns.some(c=>c.isPII)&&<span className="pii-badge">PII</span>}
                </div>
              ))}
            </div>);
          })}
        </div>
      </div>

      {/* Detail Panel */}
      {selectedDs?<div className="disc-main">
        <div className="disc-main-hdr">
          <div>
            <div style={{display:'flex',alignItems:'center',gap:8}}>
              <div className="disc-main-name">{selectedDs.name}</div>
              <span className={`disc-tier ${selectedDs.tier}`}>{selectedDs.tier}</span>
              {selectedDs.columns.some(c=>c.isPII)&&<span className="pii-badge">Contains PII</span>}
            </div>
            <div className="disc-main-meta">
              <span>📍 {selectedDs.fullPath}</span>
              <span>⚙ {selectedDs.platform}</span>
              <span>👤 {selectedDs.owner}</span>
              <span>📊 {selectedDs.rowCount.toLocaleString()} rows</span>
              <span>🕐 {selectedDs.lastUpdated}</span>
            </div>
            <div style={{fontSize:11,color:'var(--text-secondary)',marginTop:4}}>{selectedDs.description}</div>
          </div>
        </div>

        <div className="disc-tabs">
          {[{id:'columns',label:`Columns (${selectedDs.columns.length})`},{id:'profiling',label:'Profiling'},{id:'lineage',label:'Lineage'},{id:'dq',label:'DQ Rules'}].map(t=>
            <div key={t.id} className={`disc-tab ${detailTab===t.id?'active':''}`} onClick={()=>setDetailTab(t.id)}>{t.label}</div>
          )}
        </div>

        <div className="disc-body">
          {/* Columns tab */}
          {detailTab==='columns'&&<div>
            <table className="disc-col-tbl">
              <thead><tr><th>#</th><th>Column</th><th>Type</th><th>Description</th><th>PII</th><th>DQ Rules</th></tr></thead>
              <tbody>{selectedDs.columns.map((c,i)=>(
                <tr key={c.name}>
                  <td style={{color:'var(--text-muted)',fontSize:9}}>{i+1}</td>
                  <td style={{fontWeight:500,color:'var(--text-primary)'}}>{c.name}</td>
                  <td><span className="col-type">{c.type}</span></td>
                  <td style={{fontFamily:'DM Sans',fontSize:10,color:'var(--text-muted)',maxWidth:200}}>{c.description||'—'}</td>
                  <td>{c.isPII&&<span className="pii-badge">PII</span>}</td>
                  <td>{c.dqRules.length>0?c.dqRules.map((r,ri)=><span key={ri} className="dq-badge" style={{marginRight:3}}>{r}</span>):<span style={{fontSize:9,color:'var(--text-muted)'}}>none</span>}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>}

          {/* Profiling tab */}
          {detailTab==='profiling'&&<div>
            <div style={{fontSize:11,color:'var(--text-muted)',marginBottom:12}}>Column-level statistics from the latest run</div>
            <table className="disc-col-tbl">
              <thead><tr><th>Column</th><th>Type</th><th>Null %</th><th>Distinct</th><th>Distribution</th></tr></thead>
              <tbody>{selectedDs.columns.map(c=>{
                const fillW = Math.max(5,100-c.nullPct);
                return(
                <tr key={c.name}>
                  <td style={{fontWeight:500,color:'var(--text-primary)'}}>{c.name}</td>
                  <td><span className="col-type">{c.type}</span></td>
                  <td style={{color:c.nullPct>10?'var(--accent-red)':c.nullPct>5?'var(--accent-amber)':'var(--accent-green)'}}>{c.nullPct}%</td>
                  <td>{c.distinct.toLocaleString()}</td>
                  <td style={{width:120}}>
                    <div style={{display:'flex',alignItems:'center',gap:6}}>
                      <div style={{flex:1,height:6,background:'var(--bg-input)',borderRadius:3,overflow:'hidden'}}>
                        <div style={{width:`${fillW}%`,height:'100%',borderRadius:3,background:c.nullPct>10?'var(--accent-red)':c.nullPct>5?'var(--accent-amber)':'var(--accent-green)'}}/>
                      </div>
                      <span style={{fontSize:9,color:'var(--text-muted)',width:30}}>{fillW.toFixed(0)}%</span>
                    </div>
                  </td>
                </tr>);
              })}</tbody>
            </table>
          </div>}

          {/* Lineage tab */}
          {detailTab==='lineage'&&<div>
            <div style={{fontSize:11,color:'var(--text-muted)',marginBottom:12}}>Pipelines that read from or write to this dataset</div>
            {selectedDs.pipelines.map((p,i)=>(
              <div className="disc-lineage-item fade-in" key={i}>
                <span className={`disc-lineage-dir ${p.direction}`}>{p.direction==='read'?'← READ':p.direction==='write'?'→ WRITE':'↔ R/W'}</span>
                <div style={{flex:1}}>
                  <div style={{fontFamily:'JetBrains Mono',fontSize:12,fontWeight:600}}>{p.name}</div>
                  <div style={{fontSize:10,color:'var(--text-muted)'}}>Owner: {p.owner}</div>
                </div>
                <button className="btn btn-xs" onClick={()=>onNavigateToPipeline(p.id)}>Open Pipeline →</button>
              </div>
            ))}
            {selectedDs.pipelines.length===0&&<div className="disc-empty">No pipeline connections found</div>}

            {/* Visual lineage */}
            <div style={{marginTop:18}}>
              <div style={{fontSize:11,fontWeight:600,color:'var(--text-muted)',marginBottom:8,textTransform:'uppercase',letterSpacing:'.06em'}}>Flow</div>
              <div style={{display:'flex',alignItems:'center',gap:0,overflowX:'auto',padding:'8px 0'}}>
                {selectedDs.pipelines.filter(p=>p.direction==='read').length>0&&<>
                  <div style={{padding:'8px 14px',border:'1px solid rgba(34,211,238,.3)',borderRadius:'var(--radius)',background:'rgba(34,211,238,.05)',fontSize:11,fontWeight:600,color:'var(--accent-cyan)',whiteSpace:'nowrap'}}>
                    {selectedDs.name}<br/><span style={{fontSize:9,fontWeight:400,opacity:.7}}>{selectedDs.platform}</span>
                  </div>
                  <div style={{width:40,height:1,background:'var(--accent-cyan)',position:'relative',flexShrink:0}}><span style={{position:'absolute',right:-2,top:-6,color:'var(--accent-cyan)',fontSize:12}}>▸</span></div>
                </>}
                {selectedDs.pipelines.map((p,i)=>(
                  <div key={i} style={{display:'flex',alignItems:'center'}}>
                    <div style={{padding:'8px 14px',border:'1px solid var(--border)',borderRadius:'var(--radius)',background:'var(--bg-input)',fontSize:11,fontWeight:600,whiteSpace:'nowrap'}}>
                      ⚡ {p.name}
                    </div>
                    {i<selectedDs.pipelines.length-1&&<div style={{width:40,height:1,background:'var(--border)',position:'relative',flexShrink:0}}><span style={{position:'absolute',right:-2,top:-6,color:'var(--text-muted)',fontSize:12}}>▸</span></div>}
                  </div>
                ))}
                {selectedDs.pipelines.filter(p=>p.direction==='write').length>0&&<>
                  <div style={{width:40,height:1,background:'var(--accent-green)',position:'relative',flexShrink:0}}><span style={{position:'absolute',right:-2,top:-6,color:'var(--accent-green)',fontSize:12}}>▸</span></div>
                  <div style={{padding:'8px 14px',border:'1px solid rgba(52,211,153,.3)',borderRadius:'var(--radius)',background:'rgba(52,211,153,.05)',fontSize:11,fontWeight:600,color:'var(--accent-green)',whiteSpace:'nowrap'}}>
                    {selectedDs.name}<br/><span style={{fontSize:9,fontWeight:400,opacity:.7}}>target</span>
                  </div>
                </>}
              </div>
            </div>
          </div>}

          {/* DQ Rules tab */}
          {detailTab==='dq'&&<div>
            <div style={{fontSize:11,color:'var(--text-muted)',marginBottom:12}}>Quality rules applied to this dataset across all pipelines</div>
            {selectedDs.columns.filter(c=>c.dqRules.length>0).length>0?
              <table className="disc-col-tbl">
                <thead><tr><th>Column</th><th>Type</th><th>Rules</th><th>Coverage</th></tr></thead>
                <tbody>{selectedDs.columns.filter(c=>c.dqRules.length>0).map(c=>(
                  <tr key={c.name}>
                    <td style={{fontWeight:500,color:'var(--text-primary)'}}>{c.name}</td>
                    <td><span className="col-type">{c.type}</span></td>
                    <td>{c.dqRules.map((r,ri)=><span key={ri} className="dq-badge" style={{marginRight:3}}>{r}</span>)}</td>
                    <td><div style={{display:'flex',alignItems:'center',gap:6}}><div style={{width:50,height:5,background:'var(--bg-input)',borderRadius:3,overflow:'hidden'}}><div style={{width:'100%',height:'100%',background:'var(--accent-green)',borderRadius:3}}/></div><span style={{fontSize:9,color:'var(--accent-green)'}}>covered</span></div></td>
                  </tr>
                ))}</tbody>
              </table>
            :<div className="disc-empty">No DQ rules defined for this dataset</div>}

            {/* Uncovered columns */}
            {selectedDs.columns.filter(c=>c.dqRules.length===0).length>0&&<div style={{marginTop:16}}>
              <div style={{fontSize:11,fontWeight:600,color:'var(--accent-amber)',marginBottom:8}}>⚠ Columns without DQ rules ({selectedDs.columns.filter(c=>c.dqRules.length===0).length})</div>
              <div style={{display:'flex',flexWrap:'wrap',gap:6}}>
                {selectedDs.columns.filter(c=>c.dqRules.length===0).map(c=>
                  <span key={c.name} style={{padding:'4px 10px',border:'1px solid rgba(251,191,36,.2)',borderRadius:12,fontSize:10,color:'var(--accent-amber)',background:'rgba(251,191,36,.04)',fontFamily:'JetBrains Mono'}}>{c.name} <span style={{opacity:.5}}>({c.type})</span></span>
                )}
              </div>
            </div>}
          </div>}
        </div>
      </div>:<div className="disc-main"><div className="disc-empty" style={{marginTop:60}}><div style={{fontSize:28,marginBottom:8,opacity:.4}}>🔍</div>Select a dataset from the tree to explore</div></div>}
    </div>
  </div>);
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════════════════════
export default function PipelineStudio(){
  const[mode,setMode]=useState('catalog');
  const[activeStep,setActiveStep]=useState(0);
  const[showConfig,setShowConfig]=useState(false);
  const[showTemplates,setShowTemplates]=useState(false);
  const[editingPipeline,setEditingPipeline]=useState(null); // null = new, object = editing existing
  const[obsPipeline,setObsPipeline]=useState(null);
  const[config,setConfig]=useState({pipelineName:'',platform:'databricks',source:{},dqRules:[],transforms:[],sink:{},extendedSinks:[]});
  const columns=SAMPLE_COLUMNS;

  const getStatus=(i)=>{if(i===0)return config.source?.type?'completed':'';if(i===1)return(config.dqRules||[]).length>0?'completed':'';if(i===2)return(config.transforms||[]).length>0?'completed':'';if(i===3)return config.sink?.type?'completed':'';if(i===4)return(config.extendedSinks||[]).length>0?'completed':'';return '';};
  const getDetail=(i)=>{if(i===0){const st=SOURCE_TYPES.find(s=>s.value===config.source?.type);return st?.label||'Not set';}if(i===1)return`${(config.dqRules||[]).length} rules`;if(i===2)return`${(config.transforms||[]).length} steps`;if(i===3){const st=SINK_TYPES.find(s=>s.value===config.sink?.type);return st?.label||'Not set';}if(i===4)return`${(config.extendedSinks||[]).length} targets`;return '';};
  const validation=[];if(!config.pipelineName)validation.push('Name required');if(!config.source?.type)validation.push('Source required');if(!config.sink?.type)validation.push('Sink required');
  const completedCount=STEPS.filter((_,i)=>getStatus(i)==='completed').length;

  const loadPipelineForEdit=(pl)=>{
    if(pl){
      setConfig({pipelineName:pl.name,platform:pl.platform,source:pl.source,dqRules:pl.dqRules,transforms:pl.transforms,sink:pl.sink,extendedSinks:pl.extendedSinks});
      setEditingPipeline(pl);
    } else {
      setConfig({pipelineName:'',platform:'databricks',source:{},dqRules:[],transforms:[],sink:{},extendedSinks:[]});
      setEditingPipeline(null);
    }
    setActiveStep(0);setMode('builder');
  };
  const viewObservability=(pl)=>{setObsPipeline(pl);setMode('observability');};
  const applyTemplate=(t)=>{setConfig(c=>({...c,pipelineName:t.name.toLowerCase().replace(/\s+/g,'_'),source:{type:t.source,fields:{}},sink:{type:t.sink,fields:{}},dqRules:[],transforms:[],extendedSinks:t.id==='fanout'?[{id:uid(),type:'cosmosdb',fields:{}}]:[]}));setShowTemplates(false);setActiveStep(0);setMode('builder');};
  const handleAIApply=(cfg)=>{setConfig(c=>({...c,...cfg}));setMode('builder');};

  return(<><style>{css}</style>
  <div className="app">
    <div className="hdr">
      <div className="logo"><div className="logo-icon">⚡</div><div><div>Pipeline Studio</div><div className="logo-sub">Spark Scala Config Builder</div></div></div>
      <div className="hdr-actions">
        {(mode==='builder')&&<>
          <div style={{display:'flex',alignItems:'center',gap:4,padding:'4px 10px',borderRadius:6,background:editingPipeline?'rgba(251,191,36,.1)':'rgba(52,211,153,.1)',border:`1px solid ${editingPipeline?'rgba(251,191,36,.2)':'rgba(52,211,153,.2)'}`,fontSize:10,fontWeight:600,color:editingPipeline?'var(--accent-amber)':'var(--accent-green)'}}>
            {editingPipeline?`✎ Editing: ${editingPipeline.name} (${editingPipeline.version})`:'+ New Pipeline'}
          </div>
          <input className="finput" placeholder="pipeline_name" value={config.pipelineName} onChange={e=>setConfig(c=>({...c,pipelineName:e.target.value}))} style={{width:160,fontSize:11}}/>
          <select className="fsel" value={config.platform} onChange={e=>setConfig(c=>({...c,platform:e.target.value}))} style={{width:120,fontSize:11}}><option value="databricks">Databricks</option><option value="synapse">Synapse</option></select>
          <button className="btn btn-sm" onClick={()=>setShowTemplates(true)}>📋 Templates</button>
        </>}
        <button className="btn btn-sm" onClick={()=>setShowConfig(v=>!v)} style={showConfig?{borderColor:'var(--accent-cyan)',color:'var(--accent-cyan)'}:{}}>{'{ }'} Config</button>
      </div>
    </div>

    <div className="mode-bar">
      {[{id:'catalog',label:'📂 Catalog'},{id:'discovery',label:'🔍 Discovery'},{id:'builder',label:'⬡ Builder'},{id:'ai',label:'⚡ AI Assistant'},{id:'lineage',label:'⟿ Lineage'},{id:'observability',label:'📊 Observability'},{id:'approvals',label:'✓ Approvals'}].map(tab=>
        <div key={tab.id} className={`mode-tab ${mode===tab.id?'active':''}`} onClick={()=>setMode(tab.id)}>{tab.label}</div>)}
    </div>

    <div className="main">
      {mode==='catalog'&&<div style={{flex:1,overflow:'hidden'}}><CatalogPanel onEditPipeline={loadPipelineForEdit} onViewObservability={viewObservability}/></div>}

      {mode==='discovery'&&<div style={{flex:1,overflow:'hidden'}}><DiscoveryPanel onNavigateToPipeline={(plId)=>{const pl=EXISTING_PIPELINES.find(p=>p.id===plId);if(pl)loadPipelineForEdit(pl);}}/></div>}

      {mode==='builder'&&<>
        <nav className="step-nav">
          <div className="step-nav-hdr">Pipeline Steps</div>
          {STEPS.map((step,idx)=><div key={step.id} className={`sni ${idx===activeStep?'active':''} ${getStatus(idx)}`} style={idx===activeStep?{color:step.color}:{}} onClick={()=>setActiveStep(idx)}>
            <span className="sni-icon" style={idx===activeStep?{borderColor:step.color,color:step.color}:{}}>{getStatus(idx)==='completed'?'✓':step.icon}</span>
            <div><div className="sni-label">{step.label}</div><div className="sni-sub">{getDetail(idx)}</div></div></div>)}
          <div style={{flex:1}}/>
          <div style={{padding:'8px 16px',borderTop:'1px solid var(--border)'}}><div style={{fontSize:9,color:'var(--text-muted)',marginBottom:4}}>Completion</div>
            <div className="prog"><div className="prog-fill" style={{width:`${(completedCount/STEPS.length)*100}%`}}/></div><div className="prog-lbl">{completedCount}/{STEPS.length}</div></div>
        </nav>
        <div className="content">
          <div className="canvas"><div className="canvas-flow">{STEPS.map((step,idx)=><div key={step.id} style={{display:'flex',alignItems:'center'}}>
            <div className={`cn-node ${idx===activeStep?'active':''}`} style={{color:step.color,borderColor:idx===activeStep?step.color:getStatus(idx)==='completed'?'var(--accent-green)':'var(--border)'}} onClick={()=>setActiveStep(idx)}>
              <div className="cn-ico" style={{background:step.color+'15',color:step.color}}>{getStatus(idx)==='completed'?'✓':step.icon}</div>
              <div><div className="cn-lbl">{step.label}</div><div className="cn-det">{getDetail(idx)}</div></div></div>
            {idx<STEPS.length-1&&<div className={`cn-conn ${getStatus(idx)==='completed'?'done':''}`}/>}
          </div>)}</div></div>
          <div className="fpanel">
            {activeStep===0&&<SourceForm config={config} setConfig={setConfig} columns={columns}/>}
            {activeStep===1&&<DQForm config={config} setConfig={setConfig} columns={columns}/>}
            {activeStep===2&&<TransformForm config={config} setConfig={setConfig} columns={columns}/>}
            {activeStep===3&&<SinkForm config={config} setConfig={setConfig} configKey="sink"/>}
            {activeStep===4&&<ExtendedSinksForm config={config} setConfig={setConfig}/>}
          </div>
          <div className={`vbar ${validation.length>0?'err':'ok'}`}><span>{validation.length>0?'⚠':'✓'}</span>{validation.length>0?validation.join(' · '):'Config valid — ready to deploy'}</div>
          <div className="ftr"><button className="btn btn-sm" onClick={()=>setActiveStep(Math.max(0,activeStep-1))} disabled={activeStep===0}>← Prev</button>
            <div style={{display:'flex',gap:6}}>
              {editingPipeline&&<button className="btn btn-sm" style={{color:'var(--accent-amber)',borderColor:'var(--accent-amber)'}} onClick={()=>{/* submit for approval */setMode('approvals');}}>Submit for Review</button>}
              {activeStep<STEPS.length-1?<button className="btn btn-primary btn-sm" onClick={()=>setActiveStep(activeStep+1)}>Next →</button>:
                <button className="btn btn-accent btn-sm" disabled={validation.length>0}>⚡ Deploy</button>}
            </div></div>
        </div>
      </>}

      {mode==='ai'&&<div style={{flex:1,display:'flex',flexDirection:'column',overflow:'hidden'}}><AIPanel onApplyConfig={handleAIApply}/></div>}
      {mode==='lineage'&&<div style={{flex:1,overflow:'hidden'}}><LineagePanel config={config} columns={columns}/></div>}
      {mode==='observability'&&<div style={{flex:1,overflow:'hidden'}}><ObservabilityPanel selectedPipeline={obsPipeline}/></div>}
      {mode==='approvals'&&<div style={{flex:1,overflow:'hidden'}}><ApprovalPanel/></div>}
    </div>

    <div className={`cfg-drawer ${showConfig?'open':''}`}><div className="cfg-hdr"><h3>Generated Config</h3><div style={{display:'flex',gap:5}}><button className="btn btn-sm" onClick={()=>navigator.clipboard.writeText(JSON.stringify(generateConfig(config),null,2))}>Copy</button><button className="btn btn-sm btn-ghost" onClick={()=>setShowConfig(false)}>✕</button></div></div><div className="cfg-body"><ConfigJSON config={config}/></div></div>

    {showTemplates&&<div className="modal-ov" onClick={()=>setShowTemplates(false)}><div className="modal" style={{width:620}} onClick={e=>e.stopPropagation()}>
      <div className="modal-title">Start from Template</div><div className="modal-desc">Choose an archetype to pre-fill</div>
      <div className="tmpgrid">{TEMPLATES.map(t=><div className="tmpcard" key={t.id} onClick={()=>applyTemplate(t)}><div className="tmpcard-nm">{t.name}</div><div className="tmpcard-desc">{t.desc}</div><div className="tags">{t.tags.map(tag=><span className="tag" key={tag}>{tag}</span>)}</div></div>)}</div>
      <div style={{marginTop:16,textAlign:'right'}}><button className="btn btn-sm" onClick={()=>setShowTemplates(false)}>Cancel</button></div>
    </div></div>}
  </div></>);
}
