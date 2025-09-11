# COES Alert — GitHub Actions (v3)
Corre el script cada 30 minutos en GitHub Actions (ONE-SHOT) y **opcionalmente** persiste el estado en un **Gist**.

## Pasos
1. Crear repo (público) y subir estos archivos.
2. En **Settings → Secrets → Actions** agregar:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - (Opcional) `GIST_TOKEN` (PAT con scope `gist`) y `GIST_ID` (ID del gist).
3. En **Actions**, ejecutar **Run workflow** para probar.
4. El cron `*/30 * * * *` corre cada 30 min (UTC).

## Persistencia en Gist
- Crea un Gist privado con archivo `estado_alerta_chiclayo220.json` (contenido `{}`).
- Guarda el ID del gist y crea un **PAT** con **scope: gist** (guárdalo en Secrets).
- Sin Gist, el job corre, pero puede reenviar si el CM Total se mantiene sobre el umbral en corridas sucesivas.
