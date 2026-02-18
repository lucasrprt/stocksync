"""
sync_logic.py — Logique de synchronisation de stock
Street Art Magasin ↔ Shopify

Le nom catalogue du stock physique contient plusieurs informations en un seul champ :
    "CARHARTT WIP COTTON TRUNKS WHITE + WHITE I029375.931.XX"
     └─ Vendor ──┘ └─ Title propre ──────────┘ └─ Variant SKU ┘

Ce module les extrait automatiquement et les place dans les bonnes colonnes Shopify.
"""

import pandas as pd
import re
import io
import unicodedata
from collections import defaultdict
from pathlib import Path


# ── Liste des marques connues (référentiel principal) ────────
# Triée par longueur décroissante pour matcher les plus spécifiques en premier.
# Ex: "New Balance Numeric" avant "New Balance", "DC Shoes" avant "DC"
KNOWN_BRANDS_RAW = [
    "New Balance Numeric", "The Loose Company", "Deus Ex Machina",
    "Bonjour Urethane", "Bronson Speed Co", "Thrasher Seasonal",
    "The North Face", "The Quiet Life", "Converse Skate",
    "Loreak Mendian", "Poetic Collective", "Miles Griptape",
    "Beton Cire", "Bronze 56K", "Cash Only", "Film Trucks",
    "Anti Hero", "Carhartt WIP", "DC Shoes", "Last Resort Ab",
    "New Balance", "Dial Tone", "Haze Wheels", "Shake Junt",
    "Tiger Claw", "Toy Machine", "Santa Cruz", "Jason Markk",
    "Butter Goods", "Pull-In", "Hotel Blue", "Stance Socks",
    "No Name", "On Running", "Quasi", "Rave",
    "Ace", "Adidas", "Analog", "Anon", "April", "Arcade",
    "Armistice", "Birkenstock", "Blind", "Bones",
    "Broski", "Butter", "Carhartt", "Clarks", "Cliche", "Coal",
    "Commune", "Converse", "Creature", "Deus", "DGK", "Dime",
    "Eastpak", "Element", "Estime", "Fjallraven", "Gramicci",
    "Hélas", "Helas", "Herschel", "Hockey", "Huf", "Independent",
    "Isle", "Jessup", "Komono", "Krooked", "Limosine", "Magenta",
    "Mini Logo", "Neff", "Nike Sb", "Nixon", "Obey", "Palace",
    "Patagonia", "Passport", "Pizza", "Polar", "Powell",
    "Pusher", "Puma", "Rains", "Reebok", "Ripcare", "Ripndip",
    "Rvca", "Schmoove", "Sour", "Spitfire", "Stance",
    "Street Art", "Streetart", "Studio", "Stussy", "Thrasher",
    "Tired", "Veja", "Vans", "Venture", "Volcom", "Welcome",
    "Wknd", "Yardsale", "Zero", "Antiz",
]

# Dictionnaire {NOM_MAJUSCULE: casse_officielle} trié par longueur décroissante
KNOWN_BRANDS: dict = dict(
    sorted(
        {b.upper(): b for b in KNOWN_BRANDS_RAW}.items(),
        key=lambda x: len(x[0]),
        reverse=True,
    )
)


# ── Colonnes Shopify ─────────────────────────────────────────
COL_TITLE     = "Title"

COL_VENDOR    = "Vendor"
COL_BARCODE   = "Variant Barcode"
COL_QTY       = "Variant Inventory Qty"
COL_SKU       = "Variant SKU"
COL_PRICE     = "Variant Price"
COL_STATUS    = "Status"
COL_PUBLISHED = "Published"
COL_INV_TRACK = "Variant Inventory Tracker"
COL_INV_POL   = "Variant Inventory Policy"
COL_FULFILL   = "Variant Fulfillment Service"
COL_OPT1_NAME = "Option1 Name"
COL_OPT1_VAL  = "Option1 Value"


# ══════════════════════════════════════════════════════════════
# PARTIE 1 — Parsing du stock physique
# ══════════════════════════════════════════════════════════════

def parse_physical_stock(source) -> pd.DataFrame:
    """
    Parse le stock physique depuis un chemin (str/Path) ou des bytes.

    Chaque enregistrement commence par : MAGASIN;Article;Code_barre;Nom;Taille;Qte;...;Prix_vente
    Le fichier peut utiliser \\r comme fin de ligne (vieux format Mac).
    """
    raw = Path(source).read_bytes() if isinstance(source, (str, Path)) else source
    content = _decode(raw).replace("\r\n", "\n").replace("\r", "\n")

    store_marker = _detect_store(content)
    if not store_marker:
        raise ValueError(
            "Impossible de détecter le nom du magasin dans le fichier physique.\n"
            "Vérifiez que le fichier contient des lignes comme 'STREET ART;...'."
        )

    records = []
    for part in content.split(store_marker + ";")[1:]:
        fields = part.split(";")
        if len(fields) < 10:
            continue

        article    = fields[0].strip()
        code_barre = fields[1].strip()
        nom        = fields[2].strip().strip('"').strip()
        taille     = fields[3].strip().strip('"').strip()
        qte_raw    = fields[4].strip()
        pv_raw     = fields[9].strip()

        if not code_barre or code_barre.upper() == "TOTAL" or not article or not nom:
            continue
        if ";" in article[:3]:
            continue

        try:
            qte = int(float(qte_raw.replace(",", ".")))
        except (ValueError, TypeError):
            qte = 0

        try:
            prix_vente = float(pv_raw.replace(",", "."))
        except (ValueError, TypeError):
            prix_vente = 0.0

        records.append({
            "Article":       article,
            "Code_barre":    code_barre,
            "Nom_catalogue": nom,
            "Taille":        taille,
            "Qte":           qte,
            "Prix_vente":    prix_vente,
        })

    if not records:
        raise ValueError("Aucun article trouvé dans le stock physique.")

    return pd.DataFrame(records)


def _decode(raw: bytes) -> str:
    for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def _detect_store(content: str) -> str:
    m = re.search(r"([A-Z][A-Z\s]+[A-Z]);[\d_]+;[\d]+-[\d]+", content)
    if m:
        return m.group(1).strip()
    return "STREET ART" if "STREET ART;" in content else None


# ══════════════════════════════════════════════════════════════
# PARTIE 2 — Extraction des composants du nom catalogue
# ══════════════════════════════════════════════════════════════

# Pattern pour détecter un SKU fabricant en fin de nom :
# Commence par lettre ou chiffre, suivi d'au moins 3 chars alphanumériques/points/tirets
# Ex: I029375.931.XX  DB0490-010  NF00CF9C4GZ  EVYSF00150-SLK0  864349-007
_SKU_PATTERN = re.compile(r"\s+([A-Z0-9][A-Z0-9.\-]{3,})$")
_SIZES       = frozenset({"XS", "S", "M", "L", "XL", "XXL", "XXXL", "SIZE", "TAILLE"})


def extract_sku_and_title(catalog_name: str) -> tuple:
    """
    Sépare le SKU fabricant (fin de chaîne) du nom produit propre.

    Exemples :
      "CARHARTT WIP COTTON TRUNKS WHITE + WHITE I029375.931.XX"
        → ("CARHARTT WIP COTTON TRUNKS WHITE + WHITE", "I029375.931.XX")

      "NIKE SB ZOOM BLAZER MID BLACK / WHITE 864349-007"
        → ("NIKE SB ZOOM BLAZER MID BLACK / WHITE", "864349-007")

      "GX1000 FALL FLOWER COPPER 8.125 PLANCHE DE SKATE"
        → ("GX1000 FALL FLOWER COPPER 8.125 PLANCHE DE SKATE", None)

    Un SKU est identifié par :
      - Au moins un chiffre dans la séquence
      - Longueur ≥ 4 caractères
      - Pas un indicateur de taille connu (S, M, L, XL…)
    """
    name = catalog_name.strip()
    m = _SKU_PATTERN.search(name)
    if m:
        candidate = m.group(1)
        if (
            any(c.isdigit() for c in candidate)
            and candidate.upper() not in _SIZES
            and len(candidate) >= 4
        ):
            return name[: m.start()].strip(), candidate
    return name, None


def build_vendor_list(shopify_df: pd.DataFrame) -> dict:
    """
    Construit un dictionnaire fusionné {NOM_EN_MAJUSCULES: casse_officielle},
    trié par longueur décroissante pour matcher les plus longues en premier.

    Priorité :
      1. Vendors déjà dans Shopify (casse confirmée dans l'export existant)
      2. KNOWN_BRANDS (liste hardcodée, pour les marques pas encore dans Shopify)

    Cette priorité garantit que si Shopify a "Nike SB", c'est "Nike SB" qui sera
    utilisé même si KNOWN_BRANDS a "Nike Sb".

    Les valeurs placeholder ("À corriger", "") sont exclues.
    """
    # Base : liste hardcodée (pour les marques nouvelles, pas encore dans Shopify)
    merged = dict(KNOWN_BRANDS)

    # Override : vendors Shopify (casse confirmée par l'export actuel)
    vendors = shopify_df[COL_VENDOR].dropna().str.strip().unique()
    for v in vendors:
        key = v.upper()
        if v.strip() and not v.startswith("À") and v.lower() not in ("a corriger", ""):
            merged[key] = v  # Shopify prend le dessus sur KNOWN_BRANDS

    # Tri par longueur décroissante (pour matcher "NEW BALANCE NUMERIC" avant "NEW BALANCE")
    return dict(sorted(merged.items(), key=lambda x: len(x[0]), reverse=True))


def extract_vendor_and_name(title_upper: str, vendor_map: dict) -> tuple:
    """
    Extrait la marque et le nom produit depuis le titre (en majuscules).

    Utilise en priorité KNOWN_BRANDS puis les vendors Shopify, ce qui
    garantit la casse officielle : "DC Shoes", "New Balance Numeric", etc.

    Retourne (vendor: str, product_title: str).
      - vendor       : casse officielle (ex: "Carhartt WIP", "Nike Sb" → "Nike SB")
      - product_title: en Title Case (ex: "Cotton Trunks White + White")

    Si aucune marque n'est reconnue, le premier mot est utilisé comme marque.
    """
    t = title_upper.upper()

    for vendor_up, vendor_orig in vendor_map.items():
        if t.startswith(vendor_up + " ") or t == vendor_up:
            remaining = t[len(vendor_up):].strip()
            return vendor_orig, remaining.title()

    # Fallback : premier mot = marque (en Title Case)
    parts = t.split(" ", 1)
    if len(parts) == 2:
        return parts[0].title(), parts[1].title()
    return t.title(), ""


# ══════════════════════════════════════════════════════════════
# PARTIE 3 — Parsing Shopify
# ══════════════════════════════════════════════════════════════

def parse_shopify(source) -> pd.DataFrame:
    """Parse l'export CSV Shopify (séparateur virgule, format standard)."""
    if isinstance(source, (str, Path)):
        return pd.read_csv(source, dtype=str, keep_default_na=False)
    return pd.read_csv(io.BytesIO(source), dtype=str, keep_default_na=False)


# ══════════════════════════════════════════════════════════════
# PARTIE 4 — Détection carry over
# ══════════════════════════════════════════════════════════════

def normalize_name(name: str) -> str:
    """Normalise un nom pour comparaison (sans accents, minuscules, espaces unifiés)."""
    name = "".join(
        c for c in unicodedata.normalize("NFD", name)
        if unicodedata.category(c) != "Mn"
    )
    name = re.sub(r'["""\']+', "", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def get_product_id(barcode: str) -> str:
    """Extrait l'ID produit depuis un code barre (format : {product_id}-{variant})."""
    parts = barcode.split("-")
    return parts[0] if parts else barcode


def detect_carry_over(df: pd.DataFrame):
    """
    Détecte les carry over : même nom produit (sans SKU), IDs produit différents.

    En strippant le SKU avant la comparaison, on détecte correctement :
      Saison 1 : "VESTE NOIRE I029375.931.XX" (barcode=65368-2)
      Saison 2 : "VESTE NOIRE I029375.932.XX" (barcode=75368-2)  ← même produit, SKU différent !

    Retourne (df enrichi, carry_over_map).
    carry_over_map : { (norm_name, product_id) → 'S1'/'S2'/... }
    """
    df = df.copy()

    # Extraire le nom propre (sans SKU) pour la comparaison
    df["Name_no_sku"] = df["Nom_catalogue"].apply(lambda n: extract_sku_and_title(n)[0])
    df["Norm_name"]   = df["Name_no_sku"].apply(normalize_name)
    df["Product_ID"]  = df["Code_barre"].apply(get_product_id)

    name_pids = df.groupby("Norm_name")["Product_ID"].apply(
        lambda x: sorted(x.unique().tolist())
    )

    carry_over_map = {}
    for norm_name, pids in name_pids.items():
        if len(pids) > 1:
            for i, pid in enumerate(pids):
                carry_over_map[(norm_name, pid)] = f"S{i + 1}"

    return df, carry_over_map


# ══════════════════════════════════════════════════════════════
# PARTIE 5 — Synchronisation
# ══════════════════════════════════════════════════════════════

def sync_stocks(physical_df: pd.DataFrame, shopify_df: pd.DataFrame, carry_over_map: dict):
    """
    Synchronise les quantités du stock physique dans le CSV Shopify.

    Règles :
      - Match par Variant Barcode (= Code_barre physique)
      - Quantité physique → Variant Inventory Qty Shopify
      - Absent du physique → quantité mise à 0 + signalé dans le rapport
      - Carry over → suffixe S1/S2 ajouté au Title (première ligne du produit uniquement)
    """
    phys_index = {row["Code_barre"]: row for _, row in physical_df.iterrows()}
    updated    = shopify_df.copy()

    matched        = []
    qty_changes    = []
    set_to_zero    = []
    carry_over_upd = []

    for idx, row in updated.iterrows():
        barcode = str(row.get(COL_BARCODE, "")).strip()
        if not barcode:
            continue

        if barcode in phys_index:
            phys = phys_index[barcode]
            matched.append(barcode)

            # Mise à jour des quantités
            old_qty = str(row.get(COL_QTY, "0")).strip()
            new_qty = str(phys["Qte"])

            if old_qty != new_qty:
                qty_changes.append({
                    "Code barre":   barcode,
                    "Titre":        _get_title(updated, idx),
                    "Ancienne Qte": old_qty,
                    "Nouvelle Qte": new_qty,
                })
            updated.at[idx, COL_QTY] = new_qty

            # Renommage carry over (uniquement la ligne avec le titre)
            norm = phys.get("Norm_name", normalize_name(phys["Name_no_sku"] if "Name_no_sku" in phys else phys["Nom_catalogue"]))
            pid  = phys.get("Product_ID", get_product_id(barcode))
            key  = (norm, pid)

            if key in carry_over_map:
                season    = carry_over_map[key]
                cur_title = str(row.get(COL_TITLE, "")).strip()
                if cur_title and f"- {season}" not in cur_title:
                    new_title = f"{cur_title} - {season}"
                    updated.at[idx, COL_TITLE] = new_title
                    carry_over_upd.append({
                        "Code barre":    barcode,
                        "Ancien titre":  cur_title,
                        "Nouveau titre": new_title,
                    })

        else:
            # Absent du physique → quantité = 0
            old_qty = str(row.get(COL_QTY, "0")).strip()
            if old_qty not in ("0", ""):
                updated.at[idx, COL_QTY] = "0"
                set_to_zero.append({
                    "Code barre":   barcode,
                    "Titre":        _get_title(updated, idx),
                    "Ancienne Qte": old_qty,
                })

    # Produits physiques absents de Shopify
    shopify_bc  = set(shopify_df[COL_BARCODE].str.strip().unique())
    not_in_shop = []
    for _, row in physical_df.iterrows():
        bc = row["Code_barre"]
        if bc and bc not in shopify_bc:
            not_in_shop.append({
                "Code barre": bc,
                "Nom":        row["Nom_catalogue"],
                "Taille":     row["Taille"],
                "Qte":        row["Qte"],
                "Prix vente": row["Prix_vente"],
            })

    stats = {
        "total_physical":     len(physical_df),
        "total_shopify":      len(shopify_df),
        "matched":            len(matched),
        "qty_changes":        qty_changes,
        "set_to_zero":        set_to_zero,
        "not_in_shopify":     not_in_shop,
        "carry_over_updates": carry_over_upd,
    }
    return updated, stats


def _get_title(df, idx):
    t = str(df.at[idx, COL_TITLE]).strip()
    if t:
        return t
    for i in range(idx - 1, max(idx - 20, -1), -1):
        t = str(df.at[i, COL_TITLE]).strip()
        if t:
            return t
    return f"(barcode: {df.at[idx, COL_BARCODE]})"


# ══════════════════════════════════════════════════════════════
# PARTIE 6 — Génération des nouveaux produits (format Shopify exact)
# ══════════════════════════════════════════════════════════════

def generate_handle(title: str) -> str:
    """
    Génère un handle Shopify (slug URL-friendly) depuis le titre.
    Ex: "Cotton Trunks White + White" → "cotton-trunks-white-white"
    """
    h = title.lower()
    h = "".join(
        c for c in unicodedata.normalize("NFD", h)
        if unicodedata.category(c) != "Mn"
    )
    h = re.sub(r"[^a-z0-9]+", "-", h)
    return re.sub(r"-+", "-", h).strip("-")


def generate_new_products(not_in_shopify: list, shopify_df: pd.DataFrame) -> pd.DataFrame:
    """
    Génère un CSV Shopify-compatible pour les nouveaux produits.

    Pour chaque article absent de Shopify, extrait automatiquement depuis
    le nom catalogue du stock physique :
      - Vendor  (marque, matchée contre les vendors Shopify existants)
      - Title   (nom produit propre, sans la marque ni le SKU)
      - Variant SKU  (référence fabricant en fin de nom)

    Format de sortie : identique à l'export Shopify, multi-variantes correctement
    structurées (1ère ligne = données produit + 1ère variante,
    lignes suivantes = titre répété + données variante uniquement).

    Les produits sont créés en statut 'draft' pour relecture avant publication.
    """
    if not not_in_shopify:
        return pd.DataFrame()

    columns     = shopify_df.columns.tolist()
    vendor_list = build_vendor_list(shopify_df)

    # ── Étape 1 : Parser chaque article ─────────────────────
    parsed = []
    for item in not_in_shopify:
        clean_name, sku = extract_sku_and_title(item["Nom"])
        vendor, product_title = extract_vendor_and_name(clean_name, vendor_list)
        parsed.append({
            **item,
            "_clean_name":     clean_name,
            "_sku":            sku or "",
            "_vendor":         vendor,
            "_product_title":  product_title,
            "_handle":         generate_handle(product_title),
        })

    # ── Étape 2 : Grouper par produit (handle + SKU) ─────────
    # Plusieurs tailles du même produit → même groupe
    by_product = defaultdict(list)
    for item in parsed:
        key = (item["_handle"], item["_sku"])
        by_product[key].append(item)

    # ── Étape 3 : Construire les lignes Shopify ──────────────
    rows = []
    for key, variants in by_product.items():
        ref = variants[0]  # référence pour les champs produit-niveau

        for i, variant in enumerate(variants):
            row = {col: "" for col in columns}

            # Champs produit (tous remplis sur la 1ère ligne, vides ensuite)
            if i == 0:
                row[COL_TITLE]     = ref["_product_title"]
                row[COL_VENDOR]    = ref["_vendor"]
                row[COL_STATUS]    = "draft"
                row[COL_PUBLISHED] = "FALSE"
                row[COL_OPT1_NAME] = "Taille"

            else:
                # Les lignes suivantes répètent le titre (format Shopify standard)
                row[COL_TITLE] = ref["_product_title"]

            # Champs variante (toutes les lignes)
            row[COL_OPT1_VAL]  = variant["Taille"]
            row[COL_SKU]       = variant["_sku"]
            row[COL_BARCODE]   = variant["Code barre"]
            row[COL_QTY]       = str(variant["Qte"])
            row[COL_PRICE]     = str(variant["Prix vente"])
            row[COL_INV_TRACK] = "shopify"
            row[COL_INV_POL]   = "deny"
            row[COL_FULFILL]   = "manual"

            rows.append(row)

    return pd.DataFrame(rows, columns=columns)


# ══════════════════════════════════════════════════════════════
# PARTIE 7 — Rapport
# ══════════════════════════════════════════════════════════════

def generate_report(stats: dict) -> str:
    S  = "=" * 68
    S2 = "-" * 68
    L  = [
        S,
        "  RAPPORT DE SYNCHRONISATION",
        S, "",
        f"  Stock physique analysé   : {stats['total_physical']} articles",
        f"  Stock Shopify analysé    : {stats['total_shopify']} lignes",
        "",
        f"  ✅ Codes barres matchés       : {stats['matched']}",
        f"  🔄 Quantités mises à jour     : {len(stats['qty_changes'])}",
        f"  ⬇️  Mis à 0 (absent physique)  : {len(stats['set_to_zero'])}",
        f"  ➕ Nouveaux produits          : {len(stats['not_in_shopify'])}",
        f"  🔁 Carry over renommés        : {len(stats['carry_over_updates'])}",
        "",
    ]

    if stats["qty_changes"]:
        L += [S2, "CHANGEMENTS DE QUANTITÉ", S2]
        for c in stats["qty_changes"]:
            L += [f"  [{c['Code barre']}]  {str(c['Titre'])[:55]}",
                  f"      {c['Ancienne Qte']} → {c['Nouvelle Qte']}"]
        L.append("")

    if stats["set_to_zero"]:
        L += [S2, "MIS À ZÉRO (absent du stock physique)", S2]
        for c in stats["set_to_zero"]:
            L.append(f"  [{c['Code barre']}]  {str(c['Titre'])[:55]}  (était: {c['Ancienne Qte']})")
        L.append("")

    if stats["not_in_shopify"]:
        L += [S2, "NOUVEAUX PRODUITS (physique → à importer dans Shopify)", S2,
              "  → Voir nouveaux_produits.csv (Vendor, Title et SKU extraits automatiquement)", ""]
        for item in stats["not_in_shopify"]:
            # Afficher ce qui a été extrait
            clean, sku = extract_sku_and_title(item["Nom"])
            L.append(
                f"  [{item['Code barre']}]  {clean[:45]}"
                + (f"  SKU:{sku}" if sku else "")
                + f"  t:{item['Taille']}  q:{item['Qte']}"
            )
        L.append("")

    if stats["carry_over_updates"]:
        L += [S2, "CARRY OVER RENOMMÉS", S2]
        for c in stats["carry_over_updates"]:
            L += [f"  [{c['Code barre']}]",
                  f"      Avant : {c['Ancien titre']}",
                  f"      Après : {c['Nouveau titre']}", ""]
    else:
        L += [S2, "CARRY OVER", S2,
              "  Aucun carry over détecté dans ce fichier.", ""]

    L += [S, "  FIN DU RAPPORT", S]
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════

def run_sync(phys_source, shop_source) -> dict:
    """
    Exécute la synchronisation complète.

    phys_source / shop_source : chemin (str/Path) ou bytes

    Retourne un dict avec :
      'shopify_csv'      : bytes du CSV Shopify mis à jour
      'new_products_csv' : bytes du CSV nouveaux produits (vide si aucun)
      'report'           : texte du rapport
      'stats'            : dict des statistiques
    """
    physical_df            = parse_physical_stock(phys_source)
    physical_df, co_map    = detect_carry_over(physical_df)
    shopify_df             = parse_shopify(shop_source)
    shopify_updated, stats = sync_stocks(physical_df, shopify_df, co_map)

    buf = io.StringIO()
    shopify_updated.to_csv(buf, index=False)
    shopify_csv_bytes = buf.getvalue().encode("utf-8")

    new_df = generate_new_products(stats["not_in_shopify"], shopify_df)
    if not new_df.empty:
        buf2 = io.StringIO()
        new_df.to_csv(buf2, index=False)
        new_products_bytes = buf2.getvalue().encode("utf-8")
    else:
        new_products_bytes = b""

    return {
        "shopify_csv":      shopify_csv_bytes,
        "new_products_csv": new_products_bytes,
        "report":           generate_report(stats),
        "stats":            stats,
    }
