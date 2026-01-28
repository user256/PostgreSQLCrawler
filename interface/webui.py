import os
import sys
import argparse
import csv
import io
import re
import asyncio
import threading
import uuid
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional, Iterable
from pathlib import Path
from flask import Flask, jsonify, request, Response, render_template_string
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import urlparse, urlunparse

#
# Ensure project root is on sys.path so imports like src.* work when
# launching the UI from the interface/ directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>PostgreSQLCrawler</title>
  <style>
    :root {
      font-family: Inter, system-ui, -apple-system, sans-serif;
      color: #111827;
      background: #f8fafc;
    }
    body { margin: 0; }
    header {
      padding: 12px 16px;
      background: #0f172a;
      color: #e2e8f0;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    header h1 { margin: 0; font-size: 18px; }
    .conn-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    .conn-row input, .conn-row select {
      width: 140px;
      padding: 6px 8px;
      border-radius: 6px;
      border: 1px solid #334155;
      background: #0b1224;
      color: #e2e8f0;
      font-size: 12px;
    }
    .conn-row input[type="password"] { width: 120px; }
    .conn-row label {
      font-size: 12px;
      color: #cbd5e1;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .conn-row label span { font-size: 11px; }
    main { padding: 12px; overflow: auto; height: calc(100vh - 140px); }
    section { max-width: 100%; }
    h2 { margin: 0 0 8px 0; font-size: 14px; color: #0f172a; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { margin: 4px 0; }
    button { cursor: pointer; }
    .item { padding: 6px 8px; border-radius: 6px; border: 1px solid transparent; }
    .item:hover { background: #f1f5f9; }
    .item.view { border: 1px solid #e2e8f0; background: #fff; }
    .pill {
      font-size: 11px;
      padding: 2px 6px;
      border-radius: 999px;
      background: #e2e8f0;
      color: #0f172a;
      margin-left: 6px;
    }
    textarea {
      width: 100%;
      min-height: 36px;
      max-height: 200px;
      font-family: ui-monospace, SFMono-Regular, SFMono, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 13px;
      padding: 8px;
      border-radius: 8px;
      border: 1px solid #e2e8f0;
      box-sizing: border-box;
      resize: vertical;
      transition: min-height 0.2s;
    }
    textarea:focus { min-height: 120px; }
    pre {
      background: #0b1224;
      color: #e2e8f0;
      padding: 8px;
      border-radius: 6px;
      overflow: auto;
      font-size: 12px;
      margin: 0;
      display: none;
    }
    pre.expanded { display: block; }
    .view-def-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      cursor: pointer;
      padding: 4px 0;
    }
    .view-def-header:hover { color: #3b82f6; }
    .view-def-toggle { font-size: 12px; color: #64748b; }
    table { border-collapse: collapse; width: 100%; font-size: 13px; background: #fff; }
    th, td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; }
    th { background: #f8fafc; position: sticky; top: 0; }
    .actions { margin: 8px 0; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .btn {
      padding: 6px 10px;
      border-radius: 6px;
      border: 1px solid #0f172a;
      background: #0f172a;
      color: #fff;
      font-weight: 600;
    }
    .btn.secondary { background: #fff; color: #0f172a; }
    .btn.toggle { background: #fff; color: #0f172a; border-color: #e2e8f0; }
    .btn.toggle.active { background: #0f172a; color: #fff; border-color: #0f172a; }
    .status { font-size: 12px; color: #475569; }
    .error { color: #b91c1c; }
    .grid { display: grid; gap: 12px; grid-template-columns: 1fr 1fr; }
    .card {
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 10px;
      margin-bottom: 12px;
    }
    select { cursor: pointer; }
    .views-container { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
    .view-btn {
      padding: 4px 8px;
      border-radius: 4px;
      border: 1px solid #e2e8f0;
      background: #fff;
      font-size: 12px;
      cursor: pointer;
    }
    .view-btn:hover { background: #f1f5f9; }
    .view-btn.active { background: #0f172a; color: #fff; border-color: #0f172a; }
    .view-def-btn { margin-top: 8px; }
    .columns-grid {
      display: grid;
      gap: 6px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      margin-top: 6px;
    }
    .inline-inputs {
      display: grid;
      gap: 8px;
      grid-template-columns: 2fr 1fr;
      margin-top: 8px;
    }
  </style>
</head>
<body>
<header>
  <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 10px;">
    <div style="display: flex; align-items: center; gap: 8px; cursor: pointer;" onclick="toggleConnectionBar()">
      <span id="connectionToggle" style="font-size: 10px; transition: transform 0.2s;">▼</span>
      <h1 style="margin: 0;">PostgreSQLCrawler</h1>
    </div>
    <div class="status" id="metaStatus" style="margin-top: 4px;">Not connected</div>
  </div>
  <div id="connectionBar" class="conn-row">
    <label>
      <span>Backend</span>
      <select id="backendSelect" onchange="onBackendChange()">
        <option value="postgresql">PostgreSQL</option>
        <option value="sqlite">SQLite</option>
      </select>
    </label>

    <div id="pgFields" style="display: flex; gap: 8px; flex-wrap: wrap;">
      <label><span>Host</span><input id="pgHost" value="localhost" placeholder="localhost"></label>
      <label><span>Port</span><input id="pgPort" value="5432" placeholder="5432" style="width: 80px;"></label>
      <label><span>Username</span><input id="pgUser" placeholder="username"></label>
      <label><span>Password</span><input id="pgPass" type="password" placeholder="password"></label>
      <label>
        <span>Database</span>
        <div style="display: flex; align-items: center; gap: 2px;">
          <select id="pgDatabase" onchange="onDatabaseSelect()">
            <option>Loading...</option>
          </select>
          <button class="btn" onclick="refreshDatabases()" style="padding: 6px 8px; font-size: 16px; line-height: 1; min-width: auto;" title="Refresh databases">
            ⟳
          </button>
        </div>
      </label>
      <button class="btn" onclick="connectDb()" style="align-self: flex-end; margin-bottom: 2px;">Connect</button>
    </div>

    <div id="sqliteFields" style="display: none; gap: 8px; flex-wrap: wrap;">
      <label>
        <span>DB File</span>
        <div style="display: flex; align-items: center; gap: 2px;">
          <select id="sqlitePath" style="width: 280px;" onchange="onSqliteDatabaseSelect()">
            <option>Loading...</option>
          </select>
          <button class="btn" onclick="refreshDatabases()" style="padding: 6px 8px; font-size: 16px; line-height: 1; min-width: auto;" title="Refresh databases">
            ⟳
          </button>
        </div>
      </label>
      <button class="btn" onclick="connectDb()" style="align-self: flex-end; margin-bottom: 2px;">Connect</button>
    </div>
  </div>
</header>

<main>
  <section>
    <div class="card">
      <h2>Crawl Management</h2>

      <div style="margin-bottom: 12px;">
        <button class="btn" onclick="showCrawlForm()">Start New Crawl</button>
        <button class="btn secondary" onclick="showManageCrawls()">Manage Crawls</button>
        <button class="btn secondary" onclick="refreshCrawls()" title="Refresh crawl status">⟳</button>
      </div>

      <div id="crawlForm" style="display: none; margin-bottom: 12px; padding: 12px; background: #f8fafc; border-radius: 6px;">
        <h3 style="margin-top: 0; font-size: 14px;">Crawl Configuration</h3>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px;">
          <label><span>Start URL *</span><input id="crawlStartUrl" placeholder="https://example.com" style="width: 100%;"></label>
          <label><span>Max Pages</span><input id="crawlMaxPages" type="number" value="0" placeholder="0 = unlimited" style="width: 100%;"></label>
          <label><span>Max Depth</span><input id="crawlMaxDepth" type="number" value="3" style="width: 100%;"></label>
          <label><span>Concurrency</span><input id="crawlConcurrency" type="number" value="5" style="width: 100%;"></label>
          <label><span>Delay (seconds)</span><input id="crawlDelay" type="number" step="0.1" value="0.2" style="width: 100%;"></label>
          <label><span>Timeout (seconds)</span><input id="crawlTimeout" type="number" value="20" style="width: 100%;"></label>
        </div>

        <div style="margin-bottom: 8px; padding: 8px; background: #fff; border-radius: 4px; border: 1px solid #e5e7eb;">
          <strong style="font-size: 12px; display: block; margin-bottom: 4px;">Database (optional - uses current connection if not specified)</strong>

          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
            <label>
              <span>Backend</span>
              <select id="crawlDbBackend" onchange="onCrawlDbBackendChange()" style="width: 100%;">
                <option value="">Use current connection</option>
                <option value="postgresql">PostgreSQL</option>
                <option value="sqlite">SQLite</option>
              </select>
            </label>

            <label id="crawlDbPathLabel" style="display: none;"><span>DB File Path</span><input id="crawlDbPath" placeholder="/path/to/crawl.db" style="width: 100%;"></label>
            <label id="crawlPgHostLabel" style="display: none;"><span>Host</span><input id="crawlPgHost" placeholder="localhost" style="width: 100%;"></label>
            <label id="crawlPgPortLabel" style="display: none;"><span>Port</span><input id="crawlPgPort" placeholder="5432" style="width: 100%;"></label>
            <label id="crawlPgDbLabel" style="display: none;"><span>Database</span><input id="crawlPgDb" placeholder="crawler_db" style="width: 100%;"></label>
            <label id="crawlPgUserLabel" style="display: none;"><span>Username</span><input id="crawlPgUser" placeholder="username" style="width: 100%;"></label>
            <label id="crawlPgPassLabel" style="display: none;"><span>Password</span><input id="crawlPgPass" type="password" placeholder="password" style="width: 100%;"></label>
            <label id="crawlPgSchemaLabel" style="display: none;"><span>Schema</span><input id="crawlPgSchema" placeholder="public" value="public" style="width: 100%;"></label>
          </div>
        </div>

        <div style="display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px;">
          <label style="display: flex; align-items: center; gap: 4px;"><input type="checkbox" id="crawlUseJs"> Use JavaScript</label>
          <label style="display: flex; align-items: center; gap: 4px;"><input type="checkbox" id="crawlAllowExternal"> Allow External</label>
          <label style="display: flex; align-items: center; gap: 4px;"><input type="checkbox" id="crawlResetFrontier"> Reset Frontier</label>
          <label style="display: flex; align-items: center; gap: 4px;"><input type="checkbox" id="crawlRespectRobots" checked> Respect Robots.txt</label>
        </div>

        <div style="display: flex; gap: 8px;">
          <button class="btn" onclick="startCrawl()">Start Crawl</button>
          <button class="btn secondary" onclick="hideCrawlForm()">Cancel</button>
        </div>
      </div>

      <div id="crawlList"></div>
    </div>

    <div class="card">
      <h2>Views</h2>
      <div style="display: flex; gap: 8px; align-items: center; margin: 8px 0;">
        <button class="btn secondary" onclick="exportAllViews()" id="exportAllViewsBtn">Export All Views CSV</button>
        <span class="status" id="exportAllViewsStatus"></span>
      </div>
      <div class="views-container" id="viewList"></div>

      <div style="display: flex; align-items: center; gap: 8px; cursor: pointer; margin-top: 16px;" onclick="toggleTablesSection()">
        <span id="tablesToggle" style="font-size: 10px; transition: transform 0.2s;">▶</span>
        <h2 style="margin: 0;">Tables</h2>
      </div>
      <div id="tablesContainer" style="display: none;">
        <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px; margin-top: 8px;">
          <select id="tableSelect" style="width: 300px;" onchange="onTableSelect()">
            <option value="">Select a table...</option>
          </select>
          <button class="btn" onclick="toggleViewDef()" id="viewDefBtn" style="padding: 6px 10px; font-size: 12px; white-space: nowrap;">View Definition</button>
        </div>
        <pre id="viewSql" style="display: none;">Select a view to see its definition.</pre>
      </div>

      <div style="display: flex; align-items: center; gap: 8px; cursor: pointer; margin-top: 16px;" onclick="toggleQueryInput()">
        <span id="queryToggle" style="font-size: 10px; transition: transform 0.2s;">▶</span>
        <h2 style="margin: 0;">Query</h2>
      </div>
      <div id="queryInputContainer" style="display: none;">
        <div style="display: flex; gap: 8px; align-items: center; margin-top: 8px;">
          <button class="btn toggle active" id="simpleModeBtn" onclick="setQueryMode('simple')">Simple</button>
          <button class="btn toggle" id="advancedModeBtn" onclick="setQueryMode('advanced')">Advanced</button>
          <span class="status" id="viewRowCount"></span>
        </div>

        <div id="simpleQueryContainer" style="display: none; margin-top: 8px;">
          <div class="status">Simple mode builds a SELECT against the selected view/table.</div>
          <div style="margin-top: 8px;">
            <strong style="font-size: 12px; display: block; margin-bottom: 4px;">Columns</strong>
            <label style="display: flex; align-items: center; gap: 6px; font-size: 12px;">
              <input type="checkbox" id="simpleSelectAll" checked onchange="toggleSelectAllColumns()">
              Select all
            </label>
            <div id="simpleColumns" class="columns-grid"></div>
          </div>
          <div class="inline-inputs">
            <label style="display: flex; flex-direction: column; gap: 4px;">
              <span>WHERE (optional)</span>
              <input id="simpleWhere" placeholder="status_code != 200">
            </label>
            <label style="display: flex; flex-direction: column; gap: 4px;">
              <span>LIMIT (optional)</span>
              <input id="simpleLimit" placeholder="leave blank for all rows">
            </label>
          </div>
          <div class="actions">
            <button class="btn" onclick="previewSimpleQuery()">Preview</button>
            <button class="btn secondary" onclick="exportSimpleCsv()" id="simpleExportBtn">Export CSV</button>
            <span class="status" id="simpleStatus"></span>
          </div>
        </div>

        <div id="advancedQueryContainer" style="margin-top: 8px;">
          <textarea id="queryInput" spellcheck="false" placeholder="SELECT * FROM your_view" style="margin-top: 8px;"></textarea>
          <div class="actions">
          <button class="btn" onclick="runQuery()">Run</button>
          <button class="btn secondary" onclick="exportCsv()" id="exportBtn" style="display: none;">Export CSV</button>
          <span class="status" id="queryStatus"></span>
          </div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Results</h2>
      <div id="results"></div>
    </div>
  </section>
</main>

<script>
  const viewList = document.getElementById('viewList');
  const tableSelect = document.getElementById('tableSelect');
  const viewSql = document.getElementById('viewSql');
  const queryInput = document.getElementById('queryInput');
  const results = document.getElementById('results');
  const metaStatus = document.getElementById('metaStatus');
  const queryStatus = document.getElementById('queryStatus');
  const viewRowCount = document.getElementById('viewRowCount');
  const simpleColumns = document.getElementById('simpleColumns');
  const simpleSelectAll = document.getElementById('simpleSelectAll');
  const simpleWhere = document.getElementById('simpleWhere');
  const simpleLimit = document.getElementById('simpleLimit');
  const simpleStatus = document.getElementById('simpleStatus');
  const exportAllViewsStatus = document.getElementById('exportAllViewsStatus');
  const backendSelect = document.getElementById('backendSelect');
  const pgDatabase = document.getElementById('pgDatabase');

  let viewDefExpanded = false;
  let currentObjectName = null;
  let currentObjectType = null;
  let currentViewName = null;
  let lastQueryData = null;
  let crawlRefreshInterval = null;
  let currentQueryMode = 'simple';
  let lastRowCount = null;

  function showCrawlForm() {
    document.getElementById('crawlForm').style.display = 'block';
  }

  function hideCrawlForm() {
    document.getElementById('crawlForm').style.display = 'none';
  }

  function toggleQueryInput() {
    const container = document.getElementById('queryInputContainer');
    const toggle = document.getElementById('queryToggle');
    if (container.style.display === 'none') {
      container.style.display = 'block';
      toggle.textContent = '▼';
      queryInput.focus();
    } else {
      container.style.display = 'none';
      toggle.textContent = '▶';
    }
  }

  function setQueryMode(mode) {
    currentQueryMode = mode;
    document.getElementById('simpleModeBtn').classList.toggle('active', mode === 'simple');
    document.getElementById('advancedModeBtn').classList.toggle('active', mode === 'advanced');
    document.getElementById('simpleQueryContainer').style.display = mode === 'simple' ? 'block' : 'none';
    document.getElementById('advancedQueryContainer').style.display = mode === 'advanced' ? 'block' : 'none';
  }

  function extractSelectFromViewSql(sql) {
    if (!sql) return '';
    const trimmed = sql.trim();
    if (trimmed.toLowerCase().startsWith('select')) {
      return trimmed.replace(/;+\s*$/, '');
    }
    const match = trimmed.match(/create\s+(?:or\s+replace\s+)?view[\s\S]+?\s+as\s+(select[\s\S]*)/i);
    if (match && match[1]) {
      return match[1].trim().replace(/;+\s*$/, '');
    }
    return trimmed.replace(/;+\s*$/, '');
  }

  function resetSimpleColumns(columns = []) {
    simpleColumns.innerHTML = '';
    simpleSelectAll.checked = true;
    columns.forEach(col => {
      const label = document.createElement('label');
      label.style.display = 'flex';
      label.style.alignItems = 'center';
      label.style.gap = '6px';
      label.style.fontSize = '12px';
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.value = col;
      checkbox.checked = true;
      checkbox.addEventListener('change', () => {
        if (!checkbox.checked) {
          simpleSelectAll.checked = false;
        }
      });
      label.appendChild(checkbox);
      label.appendChild(document.createTextNode(col));
      simpleColumns.appendChild(label);
    });
  }

  function toggleSelectAllColumns() {
    const checked = simpleSelectAll.checked;
    document.querySelectorAll('#simpleColumns input[type="checkbox"]').forEach(cb => {
      cb.checked = checked;
    });
  }

  function getSelectedColumns() {
    const selected = [];
    document.querySelectorAll('#simpleColumns input[type="checkbox"]').forEach(cb => {
      if (cb.checked) {
        selected.push(cb.value);
      }
    });
    return selected;
  }

  function quoteIdentifier(name) {
    return `"${String(name).replace(/"/g, '""')}"`;
  }

  function buildSimpleQuery({ preview = false } = {}) {
    if (!currentObjectName) {
      throw new Error('Select a view or table first.');
    }
    const selectedColumns = getSelectedColumns();
    const columnsSql = selectedColumns.length
      ? selectedColumns.map(col => quoteIdentifier(col)).join(', ')
      : '*';
    let sql = `SELECT ${columnsSql} FROM ${quoteIdentifier(currentObjectName)}`;
    const whereClause = simpleWhere.value.trim();
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }
    const limitValue = simpleLimit.value.trim();
    if (limitValue) {
      sql += ` LIMIT ${limitValue}`;
    } else if (preview) {
      sql += ' LIMIT 100';
    }
    return sql;
  }

  async function loadSimpleMeta(name) {
    simpleStatus.textContent = 'Loading columns...';
    try {
      const columnsData = await fetchJSON('/api/view-columns?name=' + encodeURIComponent(name));
      resetSimpleColumns(columnsData.columns || []);
    } catch (e) {
      resetSimpleColumns([]);
      simpleStatus.textContent = 'Failed to load columns';
      return;
    }
    try {
      const countData = await fetchJSON('/api/view-count?name=' + encodeURIComponent(name));
      if (typeof countData.count === 'number') {
        lastRowCount = countData.count;
        const isLarge = countData.count >= 100000;
        viewRowCount.textContent = `Rows: ${formatNumber(countData.count)}${isLarge ? ' (large export may be slow)' : ''}`;
      } else {
        lastRowCount = null;
        viewRowCount.textContent = '';
      }
    } catch (e) {
      lastRowCount = null;
      viewRowCount.textContent = '';
    }
    simpleStatus.textContent = '';
  }

  async function previewSimpleQuery() {
    simpleStatus.textContent = 'Running...';
    results.innerHTML = '';
    try {
      const sql = buildSimpleQuery({ preview: true });
      const data = await fetchJSON('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql })
      });
      lastQueryData = data;
      results.innerHTML = renderTable(data.columns, data.rows);
      simpleStatus.textContent = `Rows: ${data.rows.length}${data.truncated ? ' (truncated)' : ''}`;
    } catch (e) {
      results.innerHTML = `<p class="error">${e.message}</p>`;
      simpleStatus.textContent = 'Error';
      lastQueryData = null;
    }
  }

  async function exportSimpleCsv() {
    if (!currentObjectName) {
      alert('Select a view or table first.');
      return;
    }
    if (lastRowCount !== null && lastRowCount >= 100000) {
      const proceed = confirm(`This export may be slow (${formatNumber(lastRowCount)} rows). Continue?`);
      if (!proceed) {
        return;
      }
    }
    const payload = {
      name: currentObjectName,
      columns: getSelectedColumns(),
      where: simpleWhere.value.trim() || null,
      limit: simpleLimit.value.trim() || null
    };
    try {
      const response = await fetch('/api/export-view', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.error || 'Export failed');
      }
      const blob = await response.blob();
      downloadBlob(blob, `${currentObjectName}.csv`);
    } catch (e) {
      alert('Export failed: ' + e.message);
    }
  }

  function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  function exportAllViews() {
    if (lastRowCount !== null && lastRowCount >= 100000) {
      const proceed = confirm('Exporting all views can be slow on large datasets. Continue?');
      if (!proceed) {
        exportAllViewsStatus.textContent = '';
        return;
      }
    }
    exportAllViewsStatus.textContent = 'Preparing export...';
    window.location.href = '/api/export-all-views';
    setTimeout(() => {
      exportAllViewsStatus.textContent = '';
    }, 2000);
  }

  function toggleTablesSection() {
    const container = document.getElementById('tablesContainer');
    const toggle = document.getElementById('tablesToggle');
    if (container.style.display === 'none') {
      container.style.display = 'block';
      toggle.textContent = '▼';
    } else {
      container.style.display = 'none';
      toggle.textContent = '▶';
    }
  }

  function toggleConnectionBar() {
    const container = document.getElementById('connectionBar');
    const toggle = document.getElementById('connectionToggle');
    if (container.style.display === 'none') {
      container.style.display = 'flex';
      toggle.textContent = '▼';
    } else {
      container.style.display = 'none';
      toggle.textContent = '▶';
    }
  }

  function showManageCrawls() {
    // TODO: Implement manage crawls UI
    alert('Manage Crawls feature coming soon');
  }

  async function startCrawl() {
    const config = {
      start_url: document.getElementById('crawlStartUrl').value.trim(),
      use_js: document.getElementById('crawlUseJs').checked,
      max_pages: parseInt(document.getElementById('crawlMaxPages').value) || 0,
      max_depth: parseInt(document.getElementById('crawlMaxDepth').value) || 3,
      allow_external: document.getElementById('crawlAllowExternal').checked,
      reset_frontier: document.getElementById('crawlResetFrontier').checked,
      concurrency: parseInt(document.getElementById('crawlConcurrency').value) || 5,
      delay: parseFloat(document.getElementById('crawlDelay').value) || 0.2,
      timeout: parseInt(document.getElementById('crawlTimeout').value) || 20,
      respect_robots: document.getElementById('crawlRespectRobots').checked
    };

    if (!config.start_url) {
      alert('Start URL is required');
      return;
    }

    // Add database config if specified
    const dbBackend = document.getElementById('crawlDbBackend').value;
    if (dbBackend) {
      config.db_backend = dbBackend;
      if (dbBackend === 'postgresql') {
        config.postgres_host = document.getElementById('crawlPgHost').value.trim() || 'localhost';
        config.postgres_port = parseInt(document.getElementById('crawlPgPort').value) || 5432;
        config.postgres_db = document.getElementById('crawlPgDb').value.trim();
        config.postgres_user = document.getElementById('crawlPgUser').value.trim();
        config.postgres_password = document.getElementById('crawlPgPass').value.trim();
        config.postgres_schema = document.getElementById('crawlPgSchema').value.trim() || 'public';
      } else if (dbBackend === 'sqlite') {
        config.sqlite_path = document.getElementById('crawlDbPath').value.trim();
      }
    }

    try {
      const result = await fetchJSON('/api/crawl/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
      });
      hideCrawlForm();
      refreshCrawls();

      // Refresh database list after a short delay to catch newly created databases
      setTimeout(() => {
        refreshDatabases();
      }, 2000);

      alert(`Crawl started with ID: ${result.crawl_id}`);
    } catch (e) {
      alert('Failed to start crawl: ' + e.message);
    }
  }

  async function stopCrawl(crawlId) {
    if (!confirm('Stop this crawl?')) return;
    try {
      await fetchJSON('/api/crawl/stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ crawl_id: crawlId })
      });
      refreshCrawls();
    } catch (e) {
      alert('Failed to stop crawl: ' + e.message);
    }
  }

  async function refreshCrawls() {
    try {
      const data = await fetchJSON('/api/crawl/status');
      const list = document.getElementById('crawlList');
      if (data.crawls.length === 0) {
        list.innerHTML = '';
        return;
      }

      list.innerHTML = data.crawls.map(crawl => {
        const statusColors = {
          running: '#10b981',
          completed: '#3b82f6',
          error: '#ef4444',
          stopping: '#f59e0b',
          paused: '#64748b'
        };
        const statusColor = statusColors[crawl.status] || '#64748b';
        const dbInfo = crawl.config.db_backend
          ? (crawl.config.db_backend === 'postgresql'
              ? `${crawl.config.postgres_db || 'N/A'} @ ${crawl.config.postgres_host || 'localhost'}`
              : crawl.config.sqlite_path || 'N/A')
          : 'Using default (from start URL)';

        return `<div style="padding: 8px; border: 1px solid #e5e7eb; border-radius: 6px; margin-bottom: 8px; background: #fff;">
          <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 4px;">
            <div>
              <strong style="font-size: 13px;">${crawl.config.start_url}</strong>
              <span style="font-size: 11px; padding: 2px 6px; border-radius: 999px; background: ${statusColor}20; color: ${statusColor}; margin-left: 8px;">${crawl.status}</span>
            </div>
            ${crawl.status === 'running'
              ? `<button class="btn" style="padding: 4px 8px; font-size: 11px;" onclick="stopCrawl('${crawl.id}')">Stop</button>`
              : ''}
          </div>
          <div style="font-size: 11px; color: #64748b;">
            Started: ${new Date(crawl.started_at).toLocaleString()}
            ${crawl.progress.processed > 0 ? ` | Processed: ${crawl.progress.processed}` : ''}
            ${crawl.error ? ` | Error: ${crawl.error}` : ''}
          </div>
          <div style="font-size: 11px; color: #64748b; margin-top: 4px;">
            Database: ${dbInfo}
          </div>
        </div>`;
      }).join('');
    } catch (e) {
      console.error('Failed to refresh crawls:', e);
    }
  }

  // Auto-refresh crawls every 5 seconds
  setInterval(refreshCrawls, 5000);
  refreshCrawls();

  // Auto-refresh databases every 30 seconds
  setInterval(refreshDatabases, 30000);

  // Initial database list load
  setTimeout(() => {
    refreshDatabases();
  }, 500);

  function toggleViewDef() {
    if (!currentViewName) return;
    viewDefExpanded = !viewDefExpanded;
    viewSql.style.display = viewDefExpanded ? 'block' : 'none';
  }

  function onBackendChange() {
    const backend = backendSelect.value;
    document.getElementById('pgFields').style.display = backend === 'postgresql' ? 'flex' : 'none';
    document.getElementById('sqliteFields').style.display = backend === 'sqlite' ? 'flex' : 'none';

    // Refresh database list when switching backends
    refreshDatabases();
  }

  async function fetchJSON(url, options = {}) {
    const res = await fetch(url, options);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  }

  async function listDatabases() {
    const backend = backendSelect.value;
    if (backend === 'postgresql') {
      let hasCurrentConnection = false;
      let dbUrl;

      // First, try to use the current connection
      try {
        const meta = await fetchJSON('/api/meta');
        if (meta.url && meta.url.startsWith('postgresql://')) {
          hasCurrentConnection = true;
          // Backend will use current connection, no need to send db_url
        }
      } catch (e) {
        // If meta fetch fails, fall through to form fields
      }

      // If we don't have a current connection, use form fields
      if (!hasCurrentConnection) {
        const host = document.getElementById('pgHost').value.trim() || 'localhost';
        const port = document.getElementById('pgPort').value.trim() || '5432';
        const user = document.getElementById('pgUser').value.trim();
        const password = document.getElementById('pgPass').value.trim();
        if (!user || !password) {
          pgDatabase.innerHTML = '<option>No current connection. Enter username/password first</option>';
          return;
        }
        // Use postgres as fallback if no current connection
        dbUrl = `postgresql://${user}:${encodeURIComponent(password)}@${host}:${port}/postgres`;
      }

      try {
        // If we have a current connection, don't send db_url - backend will use current connection
        // Otherwise, send the constructed db_url
        const body = hasCurrentConnection ? {} : { db_url: dbUrl };
        const data = await fetchJSON('/api/list-databases', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        if (data.databases && Array.isArray(data.databases)) {
          pgDatabase.innerHTML =
            '<option value="">Select database...</option>' +
            data.databases.map(db => `<option value="${db}">${db}</option>`).join('');
        } else {
          pgDatabase.innerHTML = '<option>No databases found</option>';
        }
      } catch (e) {
        const errorMsg = e.message || 'Unknown error';
        pgDatabase.innerHTML = `<option>Error: ${errorMsg.substring(0, 50)}</option>`;
        console.error('Database listing error:', e);
      }
    }
  }

  document.getElementById('pgUser').addEventListener('blur', listDatabases);
  document.getElementById('pgPass').addEventListener('blur', listDatabases);
  document.getElementById('pgHost').addEventListener('blur', listDatabases);
  document.getElementById('pgPort').addEventListener('blur', listDatabases);

  async function refreshDatabases() {
    try {
      const backend = backendSelect.value;
      if (backend === 'postgresql') {
        await listDatabases();
      } else if (backend === 'sqlite') {
        await listSqliteDatabases();
      }
    } catch (e) {
      console.error('Error refreshing databases:', e);
    }
  }

  async function listSqliteDatabases() {
    try {
      const data = await fetchJSON('/api/list-sqlite-databases');
      const sqlitePath = document.getElementById('sqlitePath');
      sqlitePath.innerHTML = '<option value="">Select database...</option>';

      if (data.databases && data.databases.length > 0) {
        data.databases.forEach(db => {
          const opt = document.createElement('option');
          opt.value = db.path;
          opt.textContent = db.name + (db.size ? ` (${db.size})` : '');
          sqlitePath.appendChild(opt);
        });
      } else {
        sqlitePath.innerHTML = '<option value="">No databases found</option>';
      }
    } catch (e) {
      console.error('Failed to list SQLite databases:', e);
      document.getElementById('sqlitePath').innerHTML = '<option value="">Error loading databases</option>';
    }
  }

  function onSqliteDatabaseSelect() {
    const path = document.getElementById('sqlitePath').value;
    if (path) {
      connectDb();
    }
  }

  function onDatabaseSelect() {
    // Auto-connect when database is selected
    // connectDb will use current connection if available, or form fields
    const db = document.getElementById('pgDatabase').value;
    if (db) {
      connectDb();
    }
  }

  function populateFromMeta(data) {
    const url = data.url || '';
    const backend = data.backend || '';

    // Don't populate form fields when auto-connected from command line
    // Just set the backend and let the connection be reused
    if (backend === 'postgresql') {
      backendSelect.value = 'postgresql';
      onBackendChange();

      // Don't populate form fields - just show current database if available
      try {
        const u = new URL(url);
        const dbName = u.pathname.replace('/', '');
        if (dbName) {
          pgDatabase.innerHTML = `<option value="${dbName}">${dbName} (current)</option>`;
        }
      } catch (e) {
        console.warn('Could not parse DB URL', e);
      }
    } else if (backend === 'sqlite') {
      backendSelect.value = 'sqlite';
      onBackendChange();

      // Don't populate form fields for SQLite either
      if (sqliteSelect.value !== path) {
        const opt = document.createElement('option');
        opt.value = path;
        opt.textContent = path;
        sqliteSelect.appendChild(opt);
        sqliteSelect.value = path;
      }
    }
  }

  async function loadMeta(populate = false) {
    try {
      const data = await fetchJSON('/api/meta');
      metaStatus.textContent = `${data.backend} @ ${data.url}`;

      // Collapse connection bar if auto-connected (existing connection)
      if (data.backend && data.url) {
        const connectionBar = document.getElementById('connectionBar');
        const connectionToggle = document.getElementById('connectionToggle');
        if (connectionBar && connectionToggle) {
          connectionBar.style.display = 'none';
          connectionToggle.textContent = '▶';
        }
      }

      if (populate) populateFromMeta(data);

      viewList.innerHTML = '';
      tableSelect.innerHTML = '<option value="">Select a table...</option>';
      viewRowCount.textContent = '';

      const views = data.objects.filter(o => o.type.toLowerCase().includes('view'));
      const tables = data.objects.filter(o => o.type.toLowerCase() === 'table' || o.type.toLowerCase() === 'base table');

      // Sort views, putting view_crawl_status first
      views.sort((a, b) => {
        if (a.name === 'view_crawl_status') return -1;
        if (b.name === 'view_crawl_status') return 1;
        return a.name.localeCompare(b.name);
      });
      tables.sort((a, b) => a.name.localeCompare(b.name));

      views.forEach(obj => {
        const btn = document.createElement('button');
        btn.className = 'view-btn';
        // Clean button text: remove "view_" prefix and replace "_" with spaces
        let displayName = obj.name;
        if (displayName.startsWith('view_')) {
          displayName = displayName.substring(5);
        }
        displayName = displayName.replace(/_/g, ' ');
        btn.textContent = displayName;
        btn.dataset.viewName = obj.name; // Store original name for selection
        btn.onclick = () => selectView(obj.name);
        viewList.appendChild(btn);
      });

      tables.forEach(obj => {
        const opt = document.createElement('option');
        opt.value = obj.name;
        opt.textContent = obj.name;
        tableSelect.appendChild(opt);
      });

      // Auto-select crawl_status or view_crawl_status if present (but don't expand query)
      const hasCrawlStatus = views.find(o => o.name === 'crawl_status' || o.name === 'view_crawl_status');
      if (hasCrawlStatus) {
        selectView(hasCrawlStatus.name, false);
      }
    } catch (e) {
      metaStatus.textContent = 'Failed to load metadata';
      metaStatus.classList.add('error');
      console.error(e);
    }
  }

  async function connectDb() {
    const backend = backendSelect.value;
    let dbUrl = '';
    let dbSchema = 'public';
    let useCurrentConnection = false;
    let database = '';

    if (backend === 'postgresql') {
      database = pgDatabase.value.trim();
      if (!database) {
        metaStatus.textContent = 'Please select a database';
        metaStatus.classList.add('error');
        return;
      }

      // ALWAYS try to use current connection first - send just the database name
      // This lets the backend use SQLAlchemy's URL object to properly change the database
      // IGNORE form fields if we have a current connection - they may have wrong values
      try {
        const meta = await fetchJSON('/api/meta');
        // Check if we have a valid connection (not an error response)
        if (meta && !meta.error && meta.url && meta.url.startsWith('postgresql://')) {
          useCurrentConnection = true;
          console.log('Using current connection, sending db_name:', database);
        }
      } catch (e) {
        console.warn('Could not get current connection, will use form fields:', e);
      }

      // Only use form fields if we absolutely couldn't get current connection
      if (!useCurrentConnection) {
        const host = document.getElementById('pgHost').value.trim() || 'localhost';
        const port = document.getElementById('pgPort').value.trim() || '5432';
        const user = document.getElementById('pgUser').value.trim();
        const password = document.getElementById('pgPass').value.trim();
        if (!user || !password) {
          metaStatus.textContent = 'No current connection. Please enter username, password, and select database';
          metaStatus.classList.add('error');
          return;
        }
        dbUrl = `postgresql://${user}:${encodeURIComponent(password)}@${host}:${port}/${database}`;
      }
    } else {
      const path = document.getElementById('sqlitePath').value.trim();
      if (!path) {
        metaStatus.textContent = 'Please enter SQLite database path';
        metaStatus.classList.add('error');
        return;
      }
      dbUrl = `sqlite:///${path}`;
    }

    metaStatus.textContent = 'Connecting...';
    metaStatus.classList.remove('error');

    try {
      // If using current connection, send db_name; otherwise send db_url
      const body = useCurrentConnection ? { db_name: database, db_schema: dbSchema } : { db_url: dbUrl, db_schema: dbSchema };
      console.log('Sending connection request:', useCurrentConnection ? `db_name: ${database}` : `db_url: ${dbUrl.replace(/:[^:@]+@/, ':****@')}`);

      await fetchJSON('/api/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });

      await loadMeta(false);
      metaStatus.textContent = 'Connected';
      metaStatus.classList.remove('error');
    } catch (e) {
      metaStatus.textContent = 'Connect failed: ' + e.message;
      metaStatus.classList.add('error');
      console.error(e);
    }
  }

  async function selectView(name, expandQuery = true) {
    currentViewName = name;
    currentObjectName = name;
    currentObjectType = 'view';

    // Update active state
    document.querySelectorAll('.view-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.viewName === name);
    });

    setQueryMode('simple');
    simpleWhere.value = '';
    simpleLimit.value = '';

    // Expand query input if collapsed (only if expandQuery is true)
    if (expandQuery) {
      const container = document.getElementById('queryInputContainer');
      const toggle = document.getElementById('queryToggle');
      if (container.style.display === 'none') {
        container.style.display = 'block';
        toggle.textContent = '▼';
      }
    }

    try {
      const data = await fetchJSON('/api/view-sql?name=' + encodeURIComponent(name));
      viewSql.textContent = data.sql || 'No definition found.';
      viewDefExpanded = false;
      viewSql.style.display = 'none';
      document.getElementById('viewDefBtn').style.display = 'inline-block';
      const selectSql = extractSelectFromViewSql(data.sql || '');
      if (selectSql) {
        queryInput.value = selectSql;
      }
    } catch (e) {
      viewSql.textContent = 'Error loading view definition.';
    }

    await loadSimpleMeta(name);
    await previewSimpleQuery();
  }

  async function onTableSelect() {
    const name = tableSelect.value;
    if (name) {
      currentViewName = null;
      currentObjectName = name;
      currentObjectType = 'table';

      // Clear active state from views
      document.querySelectorAll('.view-btn').forEach(btn => btn.classList.remove('active'));

      // Expand tables section if collapsed
      const tablesContainer = document.getElementById('tablesContainer');
      const tablesToggle = document.getElementById('tablesToggle');
      if (tablesContainer.style.display === 'none') {
        tablesContainer.style.display = 'block';
        tablesToggle.textContent = '▼';
      }

      setQueryMode('simple');
      simpleWhere.value = '';
      simpleLimit.value = '';
      queryInput.value = `SELECT * FROM ${name}`;
      
      // Expand query input if collapsed
      const container = document.getElementById('queryInputContainer');
      const toggle = document.getElementById('queryToggle');
      if (container.style.display === 'none') {
        container.style.display = 'block';
        toggle.textContent = '▼';
      }
      
      viewSql.textContent = 'Tables have no stored definition.';
      viewDefExpanded = false;
      viewSql.style.display = 'none';
      document.getElementById('viewDefBtn').style.display = 'none';

      await loadSimpleMeta(name);
      await previewSimpleQuery();
    }
  }

  function renderTable(columns, rows) {
    if (!rows.length) return '<p>No rows.</p>';
    const head = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
    const body = rows.map(r =>
      '<tr>' + r.map(v => `<td>${v === null ? '<em>null</em>' : String(v)}</td>`).join('') + '</tr>'
    ).join('');
    return `<div style="overflow:auto; max-height:50vh;"><table>${head}${body}</table></div>`;
  }

  async function runQuery() {
    const sql = queryInput.value;
    queryStatus.textContent = 'Running...';
    results.innerHTML = '';
    document.getElementById('exportBtn').style.display = 'none';

    try {
      const data = await fetchJSON('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql })
      });

      lastQueryData = data;
      const tableHtml = renderTable(data.columns, data.rows);
      results.innerHTML = tableHtml;

      queryStatus.textContent = `Rows: ${data.rows.length}${data.truncated ? ' (truncated)' : ''}`;

      if (data.rows.length > 0) {
        document.getElementById('exportBtn').style.display = 'inline-block';
      }
    } catch (e) {
      results.innerHTML = `<p class="error">${e.message}</p>`;
      queryStatus.textContent = 'Error';
      lastQueryData = null;
    }
  }

  function exportCsv() {
    const sql = queryInput.value.trim();
    if (!sql) return;
    fetch('/api/export-query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql })
    })
      .then(async response => {
        if (!response.ok) {
          const err = await response.json();
          throw new Error(err.error || 'Export failed');
        }
        return response.blob();
      })
      .then(blob => downloadBlob(blob, 'export.csv'))
      .catch(err => alert('Export failed: ' + err.message));
  }

  // Attempt to auto-load metadata if server already connected via CLI/env
  setQueryMode('simple');
  loadMeta(true).catch(() => {
    metaStatus.textContent = 'Not connected';
  });

  // Manage Crawls Modal
  const manageCrawlsModal = document.createElement('div');
  manageCrawlsModal.id = 'manageCrawlsModal';
  manageCrawlsModal.style.cssText = 'display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; overflow-y: auto;';
  manageCrawlsModal.onclick = function(e) {
    if (e.target === manageCrawlsModal) {
      closeManageCrawls();
    }
  };
  manageCrawlsModal.innerHTML = `
    <div style="max-width: 900px; margin: 40px auto; background: white; border-radius: 8px; padding: 24px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);" onclick="event.stopPropagation();">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
        <h2 style="margin: 0;">Manage Crawls</h2>
        <button onclick="closeManageCrawls()" style="background: #ef4444; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;">Close</button>
      </div>
      <div id="crawlsList" style="margin-top: 20px;">
        <p>Loading crawls...</p>
      </div>
    </div>
  `;
  document.body.appendChild(manageCrawlsModal);

  async function showManageCrawls() {
    manageCrawlsModal.style.display = 'block';
    await loadCrawlsList();
  }

  function closeManageCrawls() {
    manageCrawlsModal.style.display = 'none';
  }

  async function loadCrawlsList() {
    const listContainer = document.getElementById('crawlsList');
    listContainer.innerHTML = '<p>Loading crawls...</p>';
    
    try {
      const response = await fetch('/api/manage-crawls/list');
      const data = await response.json();
      
      if (data.error) {
        listContainer.innerHTML = `<p style="color: #ef4444;">Error: ${data.error}</p>`;
        return;
      }
      
      const crawls = data.crawls || [];
      const errors = data.errors || [];
      
      // Show errors if any (but still show crawls if found)
      let errorHtml = '';
      if (errors.length > 0) {
        errorHtml = `<div style="background: #fef2f2; border: 1px solid #fecaca; border-radius: 4px; padding: 12px; margin-bottom: 16px;">
          <p style="margin: 0 0 8px 0; color: #991b1b; font-weight: 600;">Warnings:</p>
          <ul style="margin: 0; padding-left: 20px; color: #991b1b;">
            ${errors.map(e => `<li>${escapeHtml(e)}</li>`).join('')}
          </ul>
        </div>`;
      }
      
      if (crawls.length === 0) {
        listContainer.innerHTML = errorHtml + '<p style="color: #64748b;">No crawls found.</p>';
        return;
      }
      
      let html = '<div style="display: grid; gap: 16px;">';
      
      for (const crawl of crawls) {
        const stats = crawl.stats || {};
        const total = stats.frontier_done || 0;
        const queued = stats.frontier_queued || 0;
        const pending = stats.frontier_pending || 0;
        const pages = stats.pages_written || 0;
        const status200 = stats.status_200 || 0;
        const statusNon200 = stats.status_non200 || 0;
        
        html += `
          <div style="border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; background: #f8fafc;">
            <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 12px;">
              <div>
                <h3 style="margin: 0 0 4px 0; font-size: 18px;">${escapeHtml(crawl.name)}</h3>
                <p style="margin: 0; color: #64748b; font-size: 12px;">
                  ${crawl.backend === 'postgresql' ? 'PostgreSQL' : 'SQLite'}
                  ${crawl.path ? ` • ${escapeHtml(crawl.path)}` : ''}
                </p>
              </div>
              <div style="display: flex; gap: 8px;">
                <button onclick="connectToCrawl('${escapeHtml(crawl.id)}', '${crawl.backend}', '${escapeHtml(crawl.name)}')"
                        style="background: #0f172a; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 12px;">
                  Open
                </button>
                <button onclick="dropCrawl('${escapeHtml(crawl.id)}', '${crawl.backend}', '${escapeHtml(crawl.name)}')" 
                        style="background: #ef4444; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 12px;">
                  Delete
                </button>
              </div>
            </div>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-top: 12px;">
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">Total URLs</div>
                <div style="font-size: 18px; font-weight: 600;">${formatNumber(stats.urls_total || 0)}</div>
              </div>
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">Done</div>
                <div style="font-size: 18px; font-weight: 600; color: #10b981;">${formatNumber(total)}</div>
              </div>
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">Queued</div>
                <div style="font-size: 18px; font-weight: 600; color: #f59e0b;">${formatNumber(queued)}</div>
              </div>
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">Pending</div>
                <div style="font-size: 18px; font-weight: 600; color: #6366f1;">${formatNumber(pending)}</div>
              </div>
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">Pages</div>
                <div style="font-size: 18px; font-weight: 600;">${formatNumber(pages)}</div>
              </div>
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">200 OK</div>
                <div style="font-size: 18px; font-weight: 600; color: #10b981;">${formatNumber(status200)}</div>
              </div>
              <div>
                <div style="font-size: 11px; color: #64748b; text-transform: uppercase; margin-bottom: 4px;">Non-200</div>
                <div style="font-size: 18px; font-weight: 600; color: #ef4444;">${formatNumber(statusNon200)}</div>
              </div>
            </div>
          </div>
        `;
      }
      
      html += '</div>';
      listContainer.innerHTML = errorHtml + html;
    } catch (error) {
      listContainer.innerHTML = `<p style="color: #ef4444;">Error loading crawls: ${error.message}</p>`;
    }
  }

  async function connectToCrawl(crawlId, backend, crawlName) {
    try {
      metaStatus.textContent = `Connecting to ${crawlName}...`;
      metaStatus.classList.remove('error');
      if (backend === 'postgresql') {
        await fetchJSON('/api/connect', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ db_name: crawlId, db_schema: 'public' })
        });
      } else {
        await fetchJSON('/api/connect', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ db_url: `sqlite:///${crawlId}`, db_schema: 'public' })
        });
      }
      closeManageCrawls();
      await loadMeta(false);
      metaStatus.textContent = `Connected to ${crawlName}`;
    } catch (e) {
      metaStatus.textContent = 'Connect failed: ' + e.message;
      metaStatus.classList.add('error');
    }
  }

  async function dropCrawl(crawlId, backend, crawlName) {
    if (!confirm(`Are you sure you want to delete "${crawlName}"?\n\nThis action cannot be undone.`)) {
      return;
    }
    
    try {
      const response = await fetch('/api/manage-crawls/drop', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({crawl_id: crawlId, backend: backend})
      });
      
      const data = await response.json();
      
      if (data.error) {
        alert(`Error: ${data.error}`);
        return;
      }
      
      alert(`Crawl "${crawlName}" deleted successfully.`);
      await loadCrawlsList();
    } catch (error) {
      alert(`Error: ${error.message}`);
    }
  }

  function formatNumber(num) {
    return num.toLocaleString();
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
</script>
</body>
</html>
"""

def get_config() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only DB browser for PostgreSQLCrawler crawl DBs.")
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DB_URL", ""),
        help="DB URL (sqlite:///... or postgresql://user:pass@host:5432/db)"
    )
    parser.add_argument(
        "--db-schema",
        default=os.environ.get("DB_SCHEMA", "public"),
        help="PostgreSQL schema (default: public)"
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", "127.0.0.1"),
        help="Bind host (default 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", 8008)),
        help="Bind port (default 8008)"
    )
    parser.add_argument(
        "--auth-token",
        default=os.environ.get("AUTH_TOKEN", ""),
        help="Optional shared secret token required via X-Auth-Token"
    )
    args = parser.parse_args()
    return args

def create_db_engine(db_url: str, db_schema: str) -> Engine:
    connect_args = {}
    if db_url.startswith("postgresql"):
        # Set a short statement timeout to avoid hanging the UI.
        connect_args["options"] = "-c statement_timeout=5000"
    engine = create_engine(db_url, connect_args=connect_args, future=True)
    if db_url.startswith("postgresql"):
        with engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {db_schema}"))
    return engine

def detect_backend(engine: Engine) -> str:
    name = engine.url.get_backend_name()
    if "postgresql" in name:
        return "postgresql"
    if "sqlite" in name:
        return "sqlite"
    return name or "unknown"

def list_databases(db_url: str) -> List[str]:
    """List available databases from a PostgreSQL server.
    Tries the current database first, then system databases (postgres, template1)
    since the user might not have access to the default 'postgres' database.
    """
    from urllib.parse import urlparse, urlunparse

    # Parse the URL to extract components
    parsed = urlparse(db_url)

    # Extract the current database name from the URL
    current_db = parsed.path.lstrip("/") if parsed.path else None

    # Try connecting to different databases - current database first, then system databases
    system_dbs = []
    if current_db:
        system_dbs.append(current_db)
    system_dbs.extend(["postgres", "template1"])

    last_error = None

    for db_name in system_dbs:
        engine = None
        try:
            # Construct URL with this database using urlunparse to preserve credentials
            test_url = urlunparse((
                parsed.scheme,
                parsed.netloc,  # This preserves username:password@host:port
                f"/{db_name}",
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            engine = create_engine(test_url, connect_args={"options": "-c statement_timeout=5000"})
            q = text("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")

            # Use begin() which works for both SQLAlchemy 1.4 and 2.0
            with engine.begin() as conn:
                result = conn.execute(q)
                rows = result.fetchall()
                databases = [r[0] for r in rows]

            if engine:
                try:
                    engine.dispose()
                except Exception:
                    pass

            return databases

        except Exception as e:  # noqa: BLE001
            last_error = e
            if engine:
                try:
                    engine.dispose()
                except Exception:
                    pass
            # Continue to next database
            continue

    # If all attempts failed, raise the last error
    if last_error:
        import traceback
        print(f"Error in list_databases (tried {', '.join(system_dbs)}): {str(last_error)}")
        print(f"Full traceback: {traceback.format_exc()}")
        raise ValueError(f"Failed to list databases: {str(last_error)}")
    else:
        raise ValueError("Failed to list databases: Unable to connect to any system database")

def list_objects(engine: Engine, schema: str) -> List[Dict[str, str]]:
    backend = detect_backend(engine)
    if backend == "postgresql":
        q = text(
            """
            SELECT table_name AS name, table_type AS type
            FROM information_schema.tables
            WHERE table_schema = :schema
            ORDER BY table_type, table_name
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(q, {"schema": schema}).fetchall()
        return [{"name": r.name, "type": r.type} for r in rows]
    else:
        q = text(
            """
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('view','table')
            ORDER BY type, name
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
        return [{"name": r.name, "type": r.type} for r in rows]

def get_view_sql(engine: Engine, name: str, schema: str) -> str:
    backend = detect_backend(engine)
    if backend == "postgresql":
        # Use information_schema.views first (safer, standard approach)
        q = text(
            """
            SELECT view_definition
            FROM information_schema.views
            WHERE table_schema = :schema
              AND table_name = :name
            """
        )
        with engine.connect() as conn:
            row = conn.execute(q, {"schema": schema, "name": name}).scalar()
        if row:
            return row

        # Fallback to pg_get_viewdef using proper identifier quoting
        q2 = text(
            """
            SELECT pg_get_viewdef(
              format('%I.%I', :schema, :name)::regclass,
              true
            )
            """
        )
        with engine.connect() as conn:
            row = conn.execute(q2, {"schema": schema, "name": name}).scalar()
        return row or ""
    else:
        q = text("SELECT sql FROM sqlite_master WHERE type='view' AND name=:name")
        with engine.connect() as conn:
            row = conn.execute(q, {"name": name}).scalar()
        return row or ""

def is_select_only(sql: str) -> bool:
    stripped = sql.strip()
    # allow a single trailing semicolon but no multiple statements
    if stripped.endswith(";"):
        stripped = stripped[:-1].strip()
    lowered = stripped.lower()
    return lowered.startswith("select") and ";" not in lowered

def ensure_limit(sql: str, limit: int = 200) -> str:
    sql_no_trailing = sql.rstrip().rstrip(";")
    lowered = sql_no_trailing.lower()

    # Check if query already has a LIMIT clause
    if " limit " in lowered:
        # Check if it's LIMIT ALL (PostgreSQL) - don't add another limit
        if " limit all" in lowered or lowered.endswith(" limit all"):
            return sql_no_trailing
        # Query already has a limit, respect it
        return sql_no_trailing

    # No limit found, add default
    return f"{sql_no_trailing} LIMIT {limit}"


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_safe_identifier(name: str) -> bool:
    return bool(IDENTIFIER_RE.match(name or ""))


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qualify_name(engine: Engine, schema: str, name: str) -> str:
    if detect_backend(engine) == "postgresql":
        return f"{quote_ident(schema)}.{quote_ident(name)}"
    return quote_ident(name)


def get_allowed_objects(engine: Engine, schema: str) -> set[str]:
    objects = list_objects(engine, schema)
    return {obj["name"] for obj in objects}


def get_object_columns(engine: Engine, schema: str, name: str) -> List[str]:
    backend = detect_backend(engine)
    if backend == "postgresql":
        q = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :name
            ORDER BY ordinal_position
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(q, {"schema": schema, "name": name}).fetchall()
        return [r[0] for r in rows]
    q = text(f'PRAGMA table_info({quote_ident(name)})')
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [r[1] for r in rows]


def get_object_count(engine: Engine, schema: str, name: str) -> int:
    qualified = qualify_name(engine, schema, name)
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}")).fetchone()
    return int(row[0]) if row else 0


def stream_csv(result) -> Iterable[str]:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(list(result.keys()))
    yield output.getvalue()
    output.seek(0)
    output.truncate(0)

    for row in result:
        writer.writerow(list(row))
        if output.tell() >= 65536:
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    if output.tell():
        yield output.getvalue()

def run_query(engine: Engine, sql: str, max_rows: int = 10000) -> Tuple[List[str], List[List[Any]], bool]:
    if not is_select_only(sql):
        raise ValueError("Only single SELECT statements are allowed, without semicolons.")

    # Check if user explicitly wants no limit (LIMIT ALL)
    sql_lower = sql.lower()
    has_limit_all = " limit all" in sql_lower or sql_lower.strip().endswith(" limit all")

    if has_limit_all:
        # Remove LIMIT ALL and fetch all rows (but still cap at a very high number for safety)
        sql_clean = sql.rstrip().rstrip(";").rstrip()
        sql_clean = sql_clean.rsplit("LIMIT", 1)[0].rstrip() if "LIMIT" in sql_clean.upper() else sql_clean
        sql = f"{sql_clean} LIMIT 1000000"  # Very high limit for "unlimited"
        max_rows = 1000000
    else:
        sql = ensure_limit(sql, max_rows)

    with engine.connect() as conn:
        result = conn.execute(text(sql))
        columns = list(result.keys())
        rows = result.fetchall()

    # Only mark as truncated if we hit the max_rows limit (and it wasn't LIMIT ALL)
    truncated = len(rows) >= max_rows and not has_limit_all

    # Convert rows to plain lists for JSON serialization
    return columns, [list(r) for r in rows], truncated

def create_app(
    initial_engine: Optional[Engine] = None,
    initial_schema: str = "public",
    auth_token: Optional[str] = None
) -> Flask:
    app = Flask(__name__)

    # Create crawl manager instance for this app
    crawl_manager = CrawlManager()
    app.crawl_manager = crawl_manager

    state: Dict[str, Any] = {"engine": initial_engine, "schema": initial_schema}

    def check_auth():
        if not auth_token:
            return True
        return request.headers.get("X-Auth-Token") == auth_token

    def unauthorized() -> Response:
        return jsonify({"error": "unauthorized"}), 401

    @app.route("/")
    def index():
        if not check_auth():
            return unauthorized()
        return render_template_string(DEFAULT_HTML)

    @app.route("/api/meta")
    def meta():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        objs = list_objects(state["engine"], state["schema"])
        return jsonify(
            {
                "backend": detect_backend(state["engine"]),
                "url": str(state["engine"].url),
                "schema": state["schema"],
                "objects": objs,
            }
        )

    @app.route("/api/view-sql")
    def view_sql():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "name required"}), 400
        sql = get_view_sql(state["engine"], name, state["schema"])
        return jsonify({"sql": sql})

    @app.route("/api/view-columns")
    def view_columns():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "name required"}), 400
        if not is_safe_identifier(name):
            return jsonify({"error": "invalid name"}), 400
        allowed = get_allowed_objects(state["engine"], state["schema"])
        if name not in allowed:
            return jsonify({"error": "unknown object"}), 400
        columns = get_object_columns(state["engine"], state["schema"], name)
        return jsonify({"columns": columns})

    @app.route("/api/view-count")
    def view_count():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "name required"}), 400
        if not is_safe_identifier(name):
            return jsonify({"error": "invalid name"}), 400
        allowed = get_allowed_objects(state["engine"], state["schema"])
        if name not in allowed:
            return jsonify({"error": "unknown object"}), 400
        try:
            count = get_object_count(state["engine"], state["schema"], name)
            return jsonify({"count": count})
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"count failed: {e}"}), 500

    @app.route("/api/query", methods=["POST"])
    def query():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        payload = request.get_json(force=True, silent=True) or {}
        sql = payload.get("sql", "")
        try:
            columns, rows, truncated = run_query(state["engine"], sql)
            return jsonify({"columns": columns, "rows": rows, "truncated": truncated})
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"query failed: {e}"}), 500

    def generate_csv(columns: List[str], rows: List[List[Any]]) -> str:
        """Generate CSV content from columns and rows."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)
        return output.getvalue()

    @app.route("/api/export-csv")
    def export_csv():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        sql = request.args.get("sql", "")
        if not sql:
            return jsonify({"error": "sql required"}), 400
        try:
            if not is_select_only(sql):
                raise ValueError("Only single SELECT statements are allowed, without semicolons.")
            with state["engine"].connect() as conn:
                result = conn.execute(text(sql))
                response = Response(
                    stream_csv(result),
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=export.csv"},
                )
                return response
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"export failed: {e}"}), 500

    @app.route("/api/export-csv-all")
    def export_csv_all():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        sql = request.args.get("sql", "")
        if not sql:
            return jsonify({"error": "sql required"}), 400
        try:
            sql_clean = sql.rstrip().rstrip(";").rstrip()
            sql_lower = sql_clean.lower()
            if " limit " in sql_lower:
                sql_clean = re.sub(r'\s+LIMIT\s+(\d+|ALL)', '', sql_clean, flags=re.IGNORECASE)

            if not is_select_only(sql_clean):
                raise ValueError("Only single SELECT statements are allowed, without semicolons.")

            with state["engine"].connect() as conn:
                result = conn.execute(text(sql_clean))
                response = Response(
                    stream_csv(result),
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=export_all.csv"},
                )
                return response
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"export failed: {e}"}), 500

    @app.route("/api/export-query", methods=["POST"])
    def export_query():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        payload = request.get_json(force=True, silent=True) or {}
        sql = payload.get("sql", "")
        if not sql:
            return jsonify({"error": "sql required"}), 400
        try:
            if not is_select_only(sql):
                raise ValueError("Only single SELECT statements are allowed, without semicolons.")
            with state["engine"].connect() as conn:
                result = conn.execute(text(sql))
                response = Response(
                    stream_csv(result),
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=export.csv"},
                )
                return response
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"export failed: {e}"}), 500

    @app.route("/api/export-view", methods=["POST"])
    def export_view():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        payload = request.get_json(force=True, silent=True) or {}
        name = payload.get("name")
        if not name:
            return jsonify({"error": "name required"}), 400
        if not is_safe_identifier(name):
            return jsonify({"error": "invalid name"}), 400
        allowed = get_allowed_objects(state["engine"], state["schema"])
        if name not in allowed:
            return jsonify({"error": "unknown object"}), 400
        columns = payload.get("columns") or []
        where_clause = payload.get("where")
        limit = payload.get("limit")

        try:
            available_columns = get_object_columns(state["engine"], state["schema"], name)
            if columns:
                columns = [c for c in columns if c in available_columns]
            select_cols = "*" if not columns else ", ".join(quote_ident(c) for c in columns)
            qualified = qualify_name(state["engine"], state["schema"], name)
            sql = f"SELECT {select_cols} FROM {qualified}"
            if where_clause:
                if ";" in where_clause:
                    raise ValueError("Invalid WHERE clause.")
                sql += f" WHERE {where_clause}"
            if limit:
                try:
                    limit_value = int(limit)
                except (ValueError, TypeError):
                    raise ValueError("Invalid LIMIT value.")
                sql += f" LIMIT {limit_value}"

            with state["engine"].connect() as conn:
                result = conn.execute(text(sql))
                response = Response(
                    stream_csv(result),
                    mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={name}.csv"},
                )
                return response
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"export failed: {e}"}), 500

    @app.route("/api/export-all-views")
    def export_all_views():
        if not check_auth():
            return unauthorized()
        if not state.get("engine"):
            return jsonify({"error": "not connected"}), 400
        try:
            objects = list_objects(state["engine"], state["schema"])
            views = [obj["name"] for obj in objects if "view" in obj["type"].lower()]
            if not views:
                return jsonify({"error": "no views found"}), 400

            import tempfile
            import zipfile

            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                zip_path = tmp.name

            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                for view_name in views:
                    qualified = qualify_name(state["engine"], state["schema"], view_name)
                    with state["engine"].connect() as conn:
                        result = conn.execute(text(f"SELECT * FROM {qualified}"))
                        with zipf.open(f"{view_name}.csv", "w") as entry:
                            output = io.StringIO()
                            writer = csv.writer(output)
                            writer.writerow(list(result.keys()))
                            entry.write(output.getvalue().encode("utf-8"))
                            output.seek(0)
                            output.truncate(0)
                            for row in result:
                                writer.writerow(list(row))
                                if output.tell() >= 65536:
                                    entry.write(output.getvalue().encode("utf-8"))
                                    output.seek(0)
                                    output.truncate(0)
                            if output.tell():
                                entry.write(output.getvalue().encode("utf-8"))

            response = Response(
                open(zip_path, "rb"),
                mimetype="application/zip",
                headers={"Content-Disposition": "attachment; filename=all_views.zip"},
            )
            response.call_on_close(lambda: os.remove(zip_path))
            return response
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"export failed: {e}"}), 500

    @app.route("/api/list-databases", methods=["POST"])
    def list_dbs():
        if not check_auth():
            return unauthorized()

        # First, try to use the current connection if it's PostgreSQL
        if state.get("engine") and detect_backend(state["engine"]) == "postgresql":
            try:
                q = text("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
                with state["engine"].connect() as conn:
                    result = conn.execute(q)
                    rows = result.fetchall()
                    databases = [r[0] for r in rows]
                return jsonify({"databases": databases})
            except Exception as e:  # noqa: BLE001
                # If current connection fails, fall through to trying db_url
                pass

        # Fallback to using provided db_url
        payload = request.get_json(force=True, silent=True) or {}
        db_url = payload.get("db_url")
        if not db_url:
            return jsonify({"error": "db_url required"}), 400

        try:
            databases = list_databases(db_url)
            return jsonify({"databases": databases})
        except ValueError as ve:
            # This is the expected exception type from list_databases
            return jsonify({"error": str(ve)}), 500
        except Exception as e:  # noqa: BLE001
            # Log the full error for debugging
            import traceback
            error_trace = traceback.format_exc()
            print(f"Error listing databases: {error_trace}")
            return jsonify({"error": f"Database listing failed: {str(e)}"}), 500

    @app.route("/api/connect", methods=["POST"])
    def connect():
        if not check_auth():
            return unauthorized()

        payload = request.get_json(force=True, silent=True) or {}
        db_url = payload.get("db_url")
        db_name = payload.get("db_name")  # Alternative: just database name
        db_schema = payload.get("db_schema", "public")

        # If db_name is provided and we have a current PostgreSQL connection, use it
        if db_name and state.get("engine") and detect_backend(state["engine"]) == "postgresql":
            try:
                # Get the current engine's URL and replace just the database name
                current_url = state["engine"].url

                # Create new URL with same credentials but different database
                # SQLAlchemy URL objects are immutable, so create a new one
                from sqlalchemy.engine.url import URL
                new_url = URL.create(
                    drivername=current_url.drivername,
                    username=current_url.username,
                    password=current_url.password,
                    host=current_url.host,
                    port=current_url.port,
                    database=db_name,
                    query=current_url.query
                )
                #new_engine = create_db_engine(str(new_url), db_schema)
                new_engine = create_db_engine(new_url.render_as_string(hide_password=False), db_schema)

                # swap in new engine/state
                old_engine = state.get("engine")
                state["engine"] = new_engine
                state["schema"] = db_schema
                if old_engine:
                    try:
                        old_engine.dispose()
                    except Exception:
                        pass

                return jsonify({"ok": True, "backend": detect_backend(new_engine), "url": str(new_engine.url), "schema": db_schema})
            except Exception as e:  # noqa: BLE001
                return jsonify({"error": f"connect failed: {e}"}), 500

        # Fallback to using db_url
        if not db_url:
            return jsonify({"error": "db_url or db_name required"}), 400

        try:
            new_engine = create_db_engine(db_url, db_schema)

            # swap in new engine/state
            old_engine = state.get("engine")
            state["engine"] = new_engine
            state["schema"] = db_schema
            if old_engine:
                try:
                    old_engine.dispose()
                except Exception:
                    pass

            return jsonify({"ok": True, "backend": detect_backend(new_engine), "url": str(new_engine.url), "schema": db_schema})
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"connect failed: {e}"}), 500

    @app.route("/api/crawl/start", methods=["POST"])
    def start_crawl():
        if not check_auth():
            return unauthorized()
        payload = request.get_json(force=True, silent=True) or {}
        start_url = payload.get("start_url")
        if not start_url:
            return jsonify({"error": "start_url required"}), 400

        crawl_id = str(uuid.uuid4())
        config = {
            "start_url": start_url,
            "use_js": payload.get("use_js", False),
            "max_pages": payload.get("max_pages", 0),
            "max_depth": payload.get("max_depth", 3),
            "allow_external": payload.get("allow_external", False),
            "reset_frontier": payload.get("reset_frontier", False),
            "concurrency": payload.get("concurrency", 5),
            "max_workers": payload.get("max_workers", 4),
            "delay": payload.get("delay", 0.2),
            "timeout": payload.get("timeout", 20),
            "user_agent": payload.get("user_agent", "default"),
            "custom_ua": payload.get("custom_ua"),
            "respect_robots": payload.get("respect_robots", True),
            "verbose": payload.get("verbose", False),
            "db_backend": payload.get("db_backend"),
            "postgres_host": payload.get("postgres_host"),
            "postgres_port": payload.get("postgres_port"),
            "postgres_db": payload.get("postgres_db"),
            "postgres_user": payload.get("postgres_user"),
            "postgres_password": payload.get("postgres_password"),
            "postgres_schema": payload.get("postgres_schema"),
            "sqlite_path": payload.get("sqlite_path"),
        }

        # If no db_backend specified, try to use current connection
        if not config.get("db_backend") and state.get("engine"):
            engine_url = state["engine"].url
            if engine_url.drivername and "postgresql" in engine_url.drivername:
                config["db_backend"] = "postgresql"
                config["postgres_host"] = engine_url.host or "localhost"
                config["postgres_port"] = engine_url.port or 5432
                config["postgres_db"] = engine_url.database
                config["postgres_user"] = engine_url.username
                # Try to get password from URL object, fallback to parsing connection string
                password = engine_url.password
                if password is None:
                    # Password might not be exposed, try parsing from rendered URL
                    try:
                        from urllib.parse import urlparse
                        full_url = engine_url.render_as_string(hide_password=False)
                        parsed = urlparse(full_url)
                        password = parsed.password or ""
                    except Exception:
                        password = ""
                config["postgres_password"] = password
            elif engine_url.drivername and "sqlite" in engine_url.drivername:
                config["db_backend"] = "sqlite"
                # SQLite URL format: sqlite:///path or sqlite:////absolute/path
                config["sqlite_path"] = engine_url.database or str(engine_url).replace("sqlite:///", "")
        try:
            app.crawl_manager.start_crawl(crawl_id, config)
            return jsonify({"crawl_id": crawl_id, "status": "started"})
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": f"Failed to start crawl: {e}"}), 500

    @app.route("/api/crawl/stop", methods=["POST"])
    def stop_crawl():
        if not check_auth():
            return unauthorized()
        payload = request.get_json(force=True, silent=True) or {}
        crawl_id = payload.get("crawl_id")
        if not crawl_id:
            return jsonify({"error": "crawl_id required"}), 400

        if app.crawl_manager.stop_crawl(crawl_id):
            return jsonify({"status": "stopping"})
        return jsonify({"error": "Crawl not found or not running"}), 404

    @app.route("/api/crawl/status", methods=["GET"])
    def get_crawl_status():
        if not check_auth():
            return unauthorized()
        crawl_id = request.args.get("crawl_id")
        if crawl_id:
            crawl_info = app.crawl_manager.get_crawl(crawl_id)
            if crawl_info:
                return jsonify(crawl_info)
            return jsonify({"error": "Crawl not found"}), 404
        else:
            # Return all crawls
            crawls = app.crawl_manager.list_crawls()
            return jsonify({"crawls": crawls})

    @app.route("/api/list-sqlite-databases", methods=["GET"])
    def list_sqlite_dbs():
        if not check_auth():
            return unauthorized()
        try:
            import os
            from pathlib import Path

            # Look for SQLite databases in common locations
            databases = []
            seen_paths = set()

            # Check data/ directory (default location)
            data_dir = Path("data")
            if data_dir.exists():
                for db_file in data_dir.glob("*_crawl.db"):
                    try:
                        abs_path = str(db_file.absolute())
                        if abs_path in seen_paths:
                            continue
                        seen_paths.add(abs_path)

                        size = os.path.getsize(db_file)
                        size_str = f"{size / 1024 / 1024:.1f} MB" if size > 1024 * 1024 else f"{size / 1024:.1f} KB"
                        databases.append({"name": db_file.stem, "path": abs_path, "size": size_str})
                    except (OSError, PermissionError) as e:
                        # Skip files we can't access
                        continue

            # Also check current directory
            try:
                for db_file in Path(".").glob("*.db"):
                    if db_file.name in ["speedtests.db"]:
                        # Exclude known non-crawl databases
                        continue
                    try:
                        abs_path = str(db_file.absolute())
                        if abs_path in seen_paths:
                            continue
                        seen_paths.add(abs_path)

                        size = os.path.getsize(db_file)
                        size_str = f"{size / 1024 / 1024:.1f} MB" if size > 1024 * 1024 else f"{size / 1024:.1f} KB"
                        databases.append({"name": db_file.stem, "path": abs_path, "size": size_str})
                    except (OSError, PermissionError):
                        continue
            except Exception:
                pass  # Ignore errors scanning current directory

            # Sort by modification time (newest first)
            try:
                databases.sort(key=lambda x: os.path.getmtime(x["path"]) if os.path.exists(x["path"]) else 0, reverse=True)
            except Exception:
                pass  # If sorting fails, just return unsorted list

            return jsonify({"databases": databases})
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": str(e)}), 500

    def get_crawl_stats_sqlite(db_path: str) -> dict:
        """Get basic stats from a SQLite crawl database."""
        import sqlite3
        if not os.path.exists(db_path):
            return None
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            stats = {}
            # Total URLs
            cur.execute("SELECT COUNT(*) FROM urls")
            stats['urls_total'] = cur.fetchone()[0] or 0
            # Frontier stats
            cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
            frontier_stats = dict(cur.fetchall())
            stats['frontier_done'] = frontier_stats.get('done', 0)
            stats['frontier_queued'] = frontier_stats.get('queued', 0)
            stats['frontier_pending'] = frontier_stats.get('pending', 0)
            # Pages written
            cur.execute("SELECT COUNT(*) FROM content")
            stats['pages_written'] = cur.fetchone()[0] or 0
            # Status codes
            cur.execute("SELECT final_status_code, COUNT(*) FROM page_metadata GROUP BY final_status_code")
            status_counts = dict(cur.fetchall())
            stats['status_200'] = status_counts.get(200, 0)
            stats['status_non200'] = sum(v for k, v in status_counts.items() if k != 200 and k is not None)
            conn.close()
            return stats
        except Exception:
            return None

    def get_crawl_stats_postgresql(db_url: str, schema: str = "public") -> dict:
        """Get basic stats from a PostgreSQL crawl database."""
        try:
            engine = create_db_engine(db_url, schema)
            with engine.connect() as conn:
                stats = {}
                # Total URLs
                result = conn.execute(text("SELECT COUNT(*) FROM urls"))
                stats['urls_total'] = result.scalar() or 0
                # Frontier stats
                result = conn.execute(text("SELECT status, COUNT(*) FROM frontier GROUP BY status"))
                frontier_stats = {row[0]: row[1] for row in result}
                stats['frontier_done'] = frontier_stats.get('done', 0)
                stats['frontier_queued'] = frontier_stats.get('queued', 0)
                stats['frontier_pending'] = frontier_stats.get('pending', 0)
                # Pages written
                result = conn.execute(text("SELECT COUNT(*) FROM content"))
                stats['pages_written'] = result.scalar() or 0
                # Status codes
                result = conn.execute(text("SELECT final_status_code, COUNT(*) FROM page_metadata GROUP BY final_status_code"))
                status_counts = {row[0]: row[1] for row in result}
                stats['status_200'] = status_counts.get(200, 0)
                stats['status_non200'] = sum(v for k, v in status_counts.items() if k != 200 and k is not None)
            engine.dispose()
            return stats
        except Exception:
            return None

    @app.route("/api/manage-crawls/list", methods=["GET"])
    def list_all_crawls():
        """List all crawl databases (PostgreSQL and SQLite) with basic stats."""
        if not check_auth():
            return unauthorized()
        try:
            crawls = []
            errors = []
            
            # Get PostgreSQL databases if we have connection info
            current_engine = state.get("engine")
            if current_engine and detect_backend(current_engine) == "postgresql":
                try:
                    databases = []
                    try:
                        q = text("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
                        with current_engine.connect() as conn:
                            result = conn.execute(q)
                            rows = result.fetchall()
                            databases = [r[0] for r in rows]
                    except Exception:
                        # Fall back to creating a new engine with explicit credentials
                        engine_url = current_engine.url.render_as_string(hide_password=False)
                        databases = list_databases(engine_url)

                    engine_url = current_engine.url.render_as_string(hide_password=False)
                    parsed = urlparse(engine_url)

                    if not databases:
                        errors.append("No PostgreSQL databases found")
                    else:
                        for db_name in databases:
                            # Skip system databases
                            if db_name in ["postgres", "template0", "template1"]:
                                continue
                            # Check if it's a crawl database (has urls and frontier tables)
                            # Construct URL with same credentials but different database using urlunparse
                            test_url = urlunparse((
                                parsed.scheme,
                                parsed.netloc,  # This preserves username:password@host:port
                                f"/{db_name}",
                                parsed.params,
                                parsed.query,
                                parsed.fragment
                            ))
                            try:
                                # Try to get stats - if it fails, it's probably not a crawl DB
                                stats = get_crawl_stats_postgresql(test_url, state.get("schema", "public"))
                                if stats is not None:
                                    crawls.append({
                                        "id": db_name,
                                        "backend": "postgresql",
                                        "name": db_name,
                                        "url": test_url,
                                        "stats": stats
                                    })
                            except Exception as e:
                                # Skip databases that don't have crawl tables or can't be accessed
                                continue
                except Exception as e:
                    # If we can't list PostgreSQL databases, continue with SQLite
                    # Log error for debugging
                    errors.append(f"Error listing PostgreSQL databases: {str(e)}")
                    import traceback
                    print(f"Error listing PostgreSQL databases: {e}")
                    print(traceback.format_exc())
            else:
                errors.append("No PostgreSQL connection available")
            
            # Get SQLite databases
            try:
                from pathlib import Path
                seen_paths = set()
                data_dir = Path("data")
                if data_dir.exists():
                    for db_file in data_dir.glob("*_crawl.db"):
                        try:
                            abs_path = str(db_file.absolute())
                            if abs_path in seen_paths:
                                continue
                            seen_paths.add(abs_path)
                            stats = get_crawl_stats_sqlite(abs_path)
                            if stats is not None:
                                crawls.append({
                                    "id": abs_path,
                                    "backend": "sqlite",
                                    "name": db_file.stem,
                                    "path": abs_path,
                                    "stats": stats
                                })
                        except Exception:
                            continue
                # Also check current directory
                for db_file in Path(".").glob("*_crawl.db"):
                    try:
                        abs_path = str(db_file.absolute())
                        if abs_path in seen_paths:
                            continue
                        seen_paths.add(abs_path)
                        stats = get_crawl_stats_sqlite(abs_path)
                        if stats is not None:
                            crawls.append({
                                "id": abs_path,
                                "backend": "sqlite",
                                "name": db_file.stem,
                                "path": abs_path,
                                "stats": stats
                            })
                    except Exception:
                        continue
            except Exception:
                pass
            
            # Also check if the current database is a crawl database
            if current_engine:
                try:
                    current_url = str(current_engine.url)
                    parsed = urlparse(current_url)
                    current_db = parsed.path.lstrip("/")
                    if current_db and current_db not in ["postgres", "template0", "template1"]:
                        # Check if current DB is already in the list
                        if not any(c["id"] == current_db for c in crawls):
                            stats = get_crawl_stats_postgresql(current_url, state.get("schema", "public"))
                            if stats is not None:
                                crawls.insert(0, {  # Add at the beginning
                                    "id": current_db,
                                    "backend": "postgresql",
                                    "name": current_db,
                                    "url": current_url,
                                    "stats": stats
                                })
                except Exception:
                    pass  # Ignore errors checking current DB
            
            return jsonify({"crawls": crawls, "errors": errors if errors else None})
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": str(e)}), 500

    @app.route("/api/manage-crawls/stats/<crawl_id>", methods=["GET"])
    def get_crawl_stats_detailed(crawl_id):
        """Get detailed stats for a specific crawl."""
        if not check_auth():
            return unauthorized()
        try:
            backend = request.args.get("backend", "sqlite")
            
            if backend == "postgresql":
                # Parse crawl_id as database name
                if state.get("engine"):
                    engine_url = str(state["engine"].url)
                    parsed = urlparse(engine_url)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    db_url = f"{base_url}/{crawl_id}"
                    stats = get_crawl_stats_postgresql(db_url, state.get("db_schema", "public"))
                    if stats:
                        return jsonify({"stats": stats})
                return jsonify({"error": "Crawl not found"}), 404
            else:
                # SQLite - crawl_id is the path
                stats = get_crawl_stats_sqlite(crawl_id)
                if stats:
                    return jsonify({"stats": stats})
                return jsonify({"error": "Crawl not found"}), 404
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": str(e)}), 500

    @app.route("/api/manage-crawls/drop", methods=["POST"])
    def drop_crawl():
        """Drop/delete a crawl database."""
        if not check_auth():
            return unauthorized()
        try:
            data = request.get_json()
            crawl_id = data.get("crawl_id")
            backend = data.get("backend", "sqlite")
            
            if not crawl_id:
                return jsonify({"error": "crawl_id required"}), 400
            
            if backend == "postgresql":
                # Drop PostgreSQL database
                if state.get("engine"):
                    engine_url = state["engine"].url.render_as_string(hide_password=False)
                    parsed = urlparse(engine_url)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    # Connect to postgres database to drop the target database
                    admin_url = f"{base_url}/postgres"
                    try:
                        # Validate database name (only alphanumeric, underscore, hyphen)
                        import re
                        if not re.match(r'^[a-zA-Z0-9_-]+$', crawl_id):
                            return jsonify({"error": "Invalid database name"}), 400
                        
                        admin_engine = create_db_engine(admin_url, "public")
                        with admin_engine.connect() as conn:
                            # DROP DATABASE must run outside transactions
                            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                            # Terminate connections to the database first (using parameterized query)
                            conn.execute(
                                text(
                                    "SELECT pg_terminate_backend(pid) "
                                    "FROM pg_stat_activity "
                                    "WHERE datname = :dbname AND pid <> pg_backend_pid()"
                                ),
                                {"dbname": crawl_id}
                            )
                            # Drop the database (identifier must be quoted and validated)
                            quoted_dbname = crawl_id.replace('"', '""')  # Escape double quotes
                            conn.execute(text(f'DROP DATABASE IF EXISTS "{quoted_dbname}"'))
                        admin_engine.dispose()
                        return jsonify({"success": True, "message": f"Database {crawl_id} dropped successfully"})
                    except Exception as e:
                        return jsonify({"error": str(e)}), 500
                return jsonify({"error": "No PostgreSQL connection available"}), 400
            else:
                # Delete SQLite file
                if os.path.exists(crawl_id):
                    try:
                        os.remove(crawl_id)
                        return jsonify({"success": True, "message": f"Database {crawl_id} deleted successfully"})
                    except Exception as e:
                        return jsonify({"error": str(e)}), 500
                return jsonify({"error": "Database file not found"}), 404
        except Exception as e:  # noqa: BLE001
            return jsonify({"error": str(e)}), 500

    return app

# Crawl management class
class CrawlManager:
    def __init__(self):
        self.crawls: Dict[str, Dict] = {}
        self.lock = threading.Lock()

    def start_crawl(self, crawl_id: str, config: Dict) -> None:
        """Start a crawl in a background thread."""
        with self.lock:
            self.crawls[crawl_id] = {
                "id": crawl_id,
                "status": "running",
                "config": config,
                "started_at": datetime.now().isoformat(),
                "progress": {"processed": 0, "enqueued": 0},
                "error": None,
                "thread": None,
            }

        def run_crawl():
            try:
                asyncio.run(self._run_crawl(crawl_id, config))
                with self.lock:
                    if crawl_id in self.crawls:
                        self.crawls[crawl_id]["status"] = "completed"
            except Exception as e:
                error_msg = str(e)
                # Ignore signal handler error in background threads - crawl still completes successfully
                if "signal only works in main thread" in error_msg:
                    with self.lock:
                        if crawl_id in self.crawls:
                            self.crawls[crawl_id]["status"] = "completed"
                            # Don't store this non-fatal error
                else:
                    with self.lock:
                        if crawl_id in self.crawls:
                            self.crawls[crawl_id]["status"] = "error"
                            self.crawls[crawl_id]["error"] = error_msg

        thread = threading.Thread(target=run_crawl, daemon=True)
        thread.start()

        with self.lock:
            self.crawls[crawl_id]["thread"] = thread

    async def _run_crawl(self, crawl_id: str, config: Dict):
        """Run the actual crawl."""
        from src.sqlitecrawler.crawl import crawl
        from src.sqlitecrawler.config import CrawlLimits, HttpConfig, get_user_agent, get_database_config
        from src.sqlitecrawler.database import DatabaseConfig, set_global_config

        # Set environment variables before calling crawl (like command line does)
        if config.get('db_backend') == 'postgresql':
            os.environ['PostgreSQLCrawler_DB_BACKEND'] = 'postgresql'
            if config.get('postgres_host'):
                os.environ['PostgreSQLCrawler_POSTGRES_HOST'] = str(config.get('postgres_host'))
            if config.get('postgres_port'):
                os.environ['PostgreSQLCrawler_POSTGRES_PORT'] = str(config.get('postgres_port'))
            if config.get('postgres_db') or config.get('postgres_database'):
                os.environ['PostgreSQLCrawler_POSTGRES_DB'] = str(config.get('postgres_db') or config.get('postgres_database'))
            if config.get('postgres_user'):
                os.environ['PostgreSQLCrawler_POSTGRES_USER'] = str(config.get('postgres_user'))
            # Always set password (even if empty) - empty password is valid
            os.environ['PostgreSQLCrawler_POSTGRES_PASSWORD'] = str(config.get('postgres_password', ''))
        elif config.get('db_backend') == 'sqlite':
            os.environ['PostgreSQLCrawler_DB_BACKEND'] = 'sqlite'
            if config.get('sqlite_path'):
                # SQLite path handling if needed
                pass

        # Also set global config (needed for some database operations)
        # Use get_database_config() which reads from environment variables we just set
        # This ensures consistency between env vars and global config
        db_config = get_database_config(config.get("start_url"))
        set_global_config(db_config)

        # Create limits
        limits = CrawlLimits(
            max_pages=config.get("max_pages", 0),
            max_depth=config.get("max_depth", 3),
            same_host_only=not config.get("allow_external", False),
        )

        # Create HTTP config
        user_agent = config.get("custom_ua") or get_user_agent(config.get("user_agent", "default"))
        http_config = HttpConfig(
            user_agent=user_agent,
            timeout=config.get("timeout", 20),
            max_concurrency=config.get("concurrency", 5),
            delay_between_requests=config.get("delay", 0.2),
            respect_robots_txt=config.get("respect_robots", True),
        )

        try:
            # Run crawl
            await crawl(
                start=config["start_url"],
                use_js=config.get("use_js", False),
                limits=limits,
                reset_frontier=config.get("reset_frontier", False),
                http_config=http_config,
                allow_external=config.get("allow_external", False),
                max_workers=config.get("max_workers", 4),
                verbose=config.get("verbose", False),
            )
            with self.lock:
                if crawl_id in self.crawls:
                    self.crawls[crawl_id]["status"] = "completed"
        except Exception as e:
            error_msg = str(e)
            # Ignore signal handler error in background threads - crawl still completes successfully
            if "signal only works in main thread" in error_msg:
                with self.lock:
                    if crawl_id in self.crawls:
                        self.crawls[crawl_id]["status"] = "completed"
                        # Don't store this non-fatal error
            else:
                with self.lock:
                    if crawl_id in self.crawls:
                        self.crawls[crawl_id]["status"] = "error"
                        self.crawls[crawl_id]["error"] = error_msg

    def stop_crawl(self, crawl_id: str) -> bool:
        """Stop a running crawl."""
        with self.lock:
            if crawl_id not in self.crawls:
                return False
            crawl_info = self.crawls[crawl_id]
            if crawl_info["status"] not in ["running", "paused"]:
                return False
            crawl_info["status"] = "stopping"

            # Store stop request - the crawl will check this
            crawl_info["stop_requested"] = True

            # Also set global shutdown flag as fallback
            try:
                import src.sqlitecrawler.crawl as crawl_module
                crawl_module.shutdown_requested = True
            except Exception:
                pass

            return True

    def _public_crawl_dict(self, c: Dict) -> Dict:
        # Return a JSON-serializable view
        out = dict(c)
        out.pop("thread", None)  # <-- remove Thread
        out.pop("stop_requested", None)  # optional: internal flag
        return out

    def get_crawl(self, crawl_id: str) -> Optional[Dict]:
        with self.lock:
            c = self.crawls.get(crawl_id)
            return self._public_crawl_dict(c) if c else None

    def list_crawls(self) -> List[Dict]:
        with self.lock:
            return [self._public_crawl_dict(c) for c in self.crawls.values()]

def main():
    args = get_config()
    initial_engine = None
    if args.db_url:
        initial_engine = create_db_engine(args.db_url, args.db_schema)
    app = create_app(initial_engine, args.db_schema, args.auth_token)
    app.run(host=args.host, port=args.port, debug=False)

if __name__ == "__main__":
    main()