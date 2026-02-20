"""
app.py — StockSync : Application web FastAPI
Synchronisation stock physique ↔ Shopify pour Street Art

Routes :
  GET  /        → Interface HTML
  POST /sync    → Reçoit deux fichiers, retourne JSON avec CSVs + rapport
"""

import base64
import traceback
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from sync_logic import run_sync

app = FastAPI(title="StockSync — Street Art", version="1.0.0")

# ══════════════════════════════════════════════════════════════
# INTERFACE HTML
# ══════════════════════════════════════════════════════════════

HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>StockSync — Street Art</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #0f0f0f;
      color: #e8e8e8;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 40px 20px;
    }

    header {
      text-align: center;
      margin-bottom: 48px;
    }

    header h1 {
      font-size: 2rem;
      font-weight: 800;
      letter-spacing: -0.03em;
      color: #e8ff00;
    }

    header p {
      color: #888;
      margin-top: 8px;
      font-size: 0.95rem;
    }

    .card {
      background: #1a1a1a;
      border: 1px solid #2a2a2a;
      border-radius: 16px;
      padding: 32px;
      width: 100%;
      max-width: 680px;
      margin-bottom: 24px;
    }

    .card h2 {
      font-size: 1rem;
      font-weight: 600;
      margin-bottom: 24px;
      color: #ccc;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .upload-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 24px;
    }

    .drop-zone {
      border: 2px dashed #333;
      border-radius: 12px;
      padding: 28px 16px;
      text-align: center;
      cursor: pointer;
      transition: all 0.2s;
      position: relative;
    }

    .drop-zone:hover, .drop-zone.drag-over {
      border-color: #e8ff00;
      background: rgba(232, 255, 0, 0.04);
    }

    .drop-zone.has-file {
      border-color: #4ade80;
      background: rgba(74, 222, 128, 0.04);
    }

    .drop-zone input[type="file"] {
      position: absolute;
      inset: 0;
      opacity: 0;
      cursor: pointer;
      width: 100%;
      height: 100%;
    }

    .drop-zone .icon {
      font-size: 2rem;
      margin-bottom: 8px;
    }

    .drop-zone .label {
      font-size: 0.8rem;
      font-weight: 600;
      color: #e8ff00;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 4px;
    }

    .drop-zone .hint {
      font-size: 0.75rem;
      color: #555;
    }

    .drop-zone .filename {
      font-size: 0.75rem;
      color: #4ade80;
      margin-top: 6px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    button[type="submit"] {
      width: 100%;
      padding: 16px;
      background: #e8ff00;
      color: #0f0f0f;
      border: none;
      border-radius: 10px;
      font-size: 1rem;
      font-weight: 700;
      cursor: pointer;
      letter-spacing: 0.02em;
      transition: opacity 0.2s;
    }

    button[type="submit"]:hover { opacity: 0.85; }
    button[type="submit"]:disabled { opacity: 0.4; cursor: not-allowed; }

    /* Stats */
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }

    .stat {
      background: #222;
      border-radius: 10px;
      padding: 16px;
      text-align: center;
    }

    .stat .value {
      font-size: 1.8rem;
      font-weight: 800;
      color: #e8ff00;
    }

    .stat .name {
      font-size: 0.72rem;
      color: #666;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-top: 4px;
    }

    /* Downloads */
    .dl-btn {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 14px 18px;
      border: 1px solid #333;
      border-radius: 10px;
      background: #222;
      color: #e8e8e8;
      cursor: pointer;
      text-decoration: none;
      font-size: 0.875rem;
      font-weight: 500;
      margin-bottom: 10px;
      transition: border-color 0.2s;
    }

    .dl-btn:hover { border-color: #e8ff00; color: #e8ff00; }

    .dl-btn .badge {
      margin-left: auto;
      font-size: 0.7rem;
      background: #333;
      padding: 3px 8px;
      border-radius: 4px;
      color: #888;
    }

    /* Report */
    pre {
      background: #111;
      border: 1px solid #222;
      border-radius: 10px;
      padding: 20px;
      font-size: 0.75rem;
      line-height: 1.6;
      overflow-x: auto;
      white-space: pre-wrap;
      word-break: break-word;
      color: #bbb;
    }

    /* Spinner */
    .spinner {
      display: none;
      width: 20px;
      height: 20px;
      border: 3px solid #0f0f0f;
      border-top-color: transparent;
      border-radius: 50%;
      animation: spin 0.6s linear infinite;
      margin: 0 auto;
    }

    @keyframes spin { to { transform: rotate(360deg); } }

    .error-msg {
      background: rgba(239,68,68,0.1);
      border: 1px solid rgba(239,68,68,0.3);
      border-radius: 10px;
      padding: 16px;
      color: #fca5a5;
      font-size: 0.85rem;
    }

    #results { display: none; }
  </style>
</head>
<body>

<header>
  <h1>⚡ StockSync</h1>
  <p>Synchronisez le stock physique avec Shopify en un clic</p>
</header>

<div class="card">
  <h2>Upload des fichiers</h2>
  <form id="syncForm">
    <div class="upload-grid">
      <div class="drop-zone" id="zone-physique">
        <input type="file" id="physique" name="physique" accept=".csv" required>
        <div class="icon">🏪</div>
        <div class="label">Stock Physique</div>
        <div class="hint">Fichier CSV du magasin</div>
        <div class="filename" id="name-physique"></div>
      </div>
      <div class="drop-zone" id="zone-shopify">
        <input type="file" id="shopify" name="shopify" accept=".csv" required>
        <div class="icon">🛒</div>
        <div class="label">Stock Shopify</div>
        <div class="hint">Export CSV Shopify</div>
        <div class="filename" id="name-shopify"></div>
      </div>
    </div>
    <button type="submit" id="syncBtn">
      <span id="btnText">Synchroniser →</span>
      <div class="spinner" id="spinner"></div>
    </button>
  </form>
  <div class="error-msg" id="errorMsg" style="display:none; margin-top:16px;"></div>
</div>

<div class="card" id="results">
  <h2>Résultats</h2>
  <div class="stats-grid" id="statsGrid"></div>

  <a class="dl-btn" id="dlRapport" href="#" download="rapport_quantites.csv">
    📊 Rapport des quantités modifiées
    <span class="badge">CSV</span>
  </a>

  <a class="dl-btn" id="dlFiltered" href="#" download="stock_en_stock.csv">
    ✅ Fichier complet sans les produits épuisés
    <span class="badge">CSV</span>
  </a>

  <a class="dl-btn" id="dlCombined" href="#" download="stock_complet.csv">
    ⭐ Fichier complet (tous produits, nouveaux en haut)
    <span class="badge">CSV</span>
  </a>

  <a class="dl-btn" id="dlShopify" href="#" download="stock_shopify_updated.csv">
    📥 Shopify mis à jour uniquement
    <span class="badge">CSV</span>
  </a>

  <a class="dl-btn" id="dlNew" href="#" download="nouveaux_produits.csv" style="display:none">
    ➕ Nouveaux produits uniquement
    <span class="badge">CSV</span>
  </a>

  <h2 style="margin-top:24px; margin-bottom:12px;">Rapport</h2>
  <pre id="reportText"></pre>
</div>

<script>
  // Drag & drop zones
  ['physique', 'shopify'].forEach(id => {
    const zone  = document.getElementById('zone-' + id);
    const input = document.getElementById(id);
    const name  = document.getElementById('name-' + id);

    input.addEventListener('change', () => {
      if (input.files[0]) {
        name.textContent = input.files[0].name;
        zone.classList.add('has-file');
      }
    });

    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
      e.preventDefault();
      zone.classList.remove('drag-over');
      input.files = e.dataTransfer.files;
      if (input.files[0]) {
        name.textContent = input.files[0].name;
        zone.classList.add('has-file');
      }
    });
  });

  document.getElementById('syncForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn     = document.getElementById('syncBtn');
    const btnText = document.getElementById('btnText');
    const spinner = document.getElementById('spinner');
    const errDiv  = document.getElementById('errorMsg');
    const results = document.getElementById('results');

    errDiv.style.display  = 'none';
    results.style.display = 'none';
    btn.disabled    = true;
    btnText.style.display = 'none';
    spinner.style.display = 'block';

    try {
      const fd = new FormData(e.target);
      const resp = await fetch('/sync', { method: 'POST', body: fd });
      const data = await resp.json();

      if (!resp.ok) {
        throw new Error(data.detail || 'Erreur serveur');
      }

      // Stats
      const s = data.stats;
      const statsEl = document.getElementById('statsGrid');
      statsEl.innerHTML = [
        { v: s.matched,                           n: 'Matchés'        },
        { v: s.qty_changes.length,                n: 'Qtés modifiées' },
        { v: s.set_to_zero.length,                n: 'Mis à 0'        },
        { v: s.not_in_shopify.length,             n: 'Nouveaux'       },
      ].map(x => `<div class="stat"><div class="value">${x.v}</div><div class="name">${x.n}</div></div>`).join('');

      // Rapport CSV
      document.getElementById('dlRapport').href = 'data:text/csv;base64,' + data.rapport_csv_b64;

      // Fichier filtré (sans produits épuisés)
      document.getElementById('dlFiltered').href = 'data:text/csv;base64,' + data.filtered_csv_b64;

      // Fichier complet (nouveaux en haut + Shopify mis à jour)
      document.getElementById('dlCombined').href = 'data:text/csv;base64,' + data.combined_csv_b64;

      // Shopify mis à jour seul
      document.getElementById('dlShopify').href = 'data:text/csv;base64,' + data.shopify_csv_b64;

      // Nouveaux produits seuls
      const dlN = document.getElementById('dlNew');
      if (data.has_new_products) {
        dlN.href = 'data:text/csv;base64,' + data.new_products_csv_b64;
        dlN.style.display = 'flex';
      } else {
        dlN.style.display = 'none';
      }

      // Report
      document.getElementById('reportText').textContent = data.report;
      results.style.display = 'block';

    } catch (err) {
      errDiv.textContent  = '❌ ' + err.message;
      errDiv.style.display = 'block';
    } finally {
      btn.disabled = false;
      btnText.style.display = 'inline';
      spinner.style.display = 'none';
    }
  });
</script>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


@app.post("/sync")
async def sync(
    physique: UploadFile = File(..., description="Stock physique CSV"),
    shopify:  UploadFile = File(..., description="Stock Shopify CSV (export Shopify)"),
):
    try:
        phys_bytes = await physique.read()
        shop_bytes = await shopify.read()

        result = run_sync(phys_bytes, shop_bytes)

        shopify_b64   = base64.b64encode(result["shopify_csv"]).decode()
        new_prod_b64  = base64.b64encode(result["new_products_csv"]).decode() if result["new_products_csv"] else ""
        combined_b64  = base64.b64encode(result["combined_csv"]).decode()
        filtered_b64  = base64.b64encode(result["filtered_csv"]).decode()
        rapport_b64   = base64.b64encode(result["rapport_csv"]).decode()

        # Convertir les stats pour la sérialisation JSON
        stats = {k: (v if not isinstance(v, list) else v)
                 for k, v in result["stats"].items()}

        return JSONResponse({
            "shopify_csv_b64":      shopify_b64,
            "new_products_csv_b64": new_prod_b64,
            "combined_csv_b64":     combined_b64,
            "filtered_csv_b64":     filtered_b64,
            "rapport_csv_b64":      rapport_b64,
            "has_new_products":     bool(result["new_products_csv"]),
            "report":               result["report"],
            "stats":                stats,
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e) + "\n" + traceback.format_exc())
