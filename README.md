# StockSync — Street Art

Application web de synchronisation du stock physique ↔ Shopify.

## Fonctionnement

1. Uploadez le CSV du stock physique (export de votre logiciel de caisse)
2. Uploadez l'export CSV de Shopify
3. Cliquez **Synchroniser**
4. Téléchargez le CSV Shopify mis à jour + le rapport

### Ce que fait le script

- **Match** chaque article par code barre (`{product_id}-{variant}`)
- **Met à jour** les quantités Shopify avec les quantités physiques
- **Met à 0** les articles présents sur Shopify mais absents du physique
- **Détecte les carry over** (même produit, saison différente) et les renomme S1/S2/...
- **Génère un CSV** pour les nouveaux produits (absent de Shopify), avec extraction automatique du Vendor, Title et SKU depuis le nom catalogue

## Déploiement

### Railway (recommandé)

1. Créer un compte sur [railway.app](https://railway.app)
2. "New Project" → "Deploy from GitHub repo" → sélectionner ce repo
3. Railway détecte automatiquement le `Procfile` et déploie
4. URL de production disponible dans l'onglet "Settings"

### Render

1. Créer un compte sur [render.com](https://render.com)
2. "New Web Service" → connecter ce repo GitHub
3. Build Command : `pip install -r requirements.txt`
4. Start Command : `uvicorn app:app --host 0.0.0.0 --port $PORT`

## Développement local

```bash
pip install -r requirements.txt
uvicorn app:app --reload
# → http://localhost:8000
```

## Protection par mot de passe (optionnel)

Ajouter dans `app.py` avant les routes :

```python
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

security = HTTPBasic()

def check_auth(credentials: HTTPBasicCredentials = Depends(security)):
    ok = (
        secrets.compare_digest(credentials.username, "streetart") and
        secrets.compare_digest(credentials.password, "votre-mot-de-passe")
    )
    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            headers={"WWW-Authenticate": "Basic"})
    return credentials
```

Puis ajouter `Depends(check_auth)` aux routes : `async def index(auth=Depends(check_auth)):`
