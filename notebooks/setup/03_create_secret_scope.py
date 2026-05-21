# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — create the `lol-analytics` secret scope
# MAGIC
# MAGIC Run this **once per workspace**. It creates a Databricks-backed
# MAGIC secret scope named `lol-analytics` and stores the Riot API key in
# MAGIC it as `riot-api-key`. After this, the Bronze ingestion notebooks
# MAGIC read the key with `dbutils.secrets.get(scope="lol-analytics",
# MAGIC key="riot-api-key")` — the key never appears in notebook code,
# MAGIC output, or git.
# MAGIC
# MAGIC **Why a notebook and not the CLI:** the Databricks CLI also works
# MAGIC (`databricks secrets create-scope ...`), but doing it from a
# MAGIC notebook needs nothing installed locally — it uses the workspace's
# MAGIC own REST API with the notebook's session token.
# MAGIC
# MAGIC **Security notes:**
# MAGIC - The key is entered through a `password`-type widget, so it is
# MAGIC   masked in the notebook UI.
# MAGIC - This notebook never prints the key — only the status of the REST
# MAGIC   calls. The key is not stored in the `.py` file (it comes from the
# MAGIC   widget at run time), so committing this notebook is safe.
# MAGIC - After running, clear the widget value (Edit → clear, or just
# MAGIC   detach the notebook) so the key does not linger in the session.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Enter the Riot API key
# MAGIC
# MAGIC Paste your key (`RGAPI-...`) into the **riot_api_key** widget that
# MAGIC appears at the top of the notebook. Dev keys expire every 24h —
# MAGIC use a fresh one. The widget is `password`-type, so the value is
# MAGIC masked.

# COMMAND ----------

dbutils.widgets.text("riot_api_key", "", "Riot API key (RGAPI-...)")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create the secret scope and store the key
# MAGIC
# MAGIC Uses the workspace REST API with the notebook's own session token.
# MAGIC Both calls are idempotent: re-running re-creates nothing harmful —
# MAGIC `create-scope` returns an error if the scope already exists (which
# MAGIC we tolerate), and `put-secret` overwrites the stored value.

# COMMAND ----------

import requests

SCOPE = "lol-analytics"
SECRET_KEY = "riot-api-key"

api_key = dbutils.widgets.get("riot_api_key").strip()  # noqa: F821
if not api_key:
    raise ValueError(
        "The riot_api_key widget is empty. Paste your RGAPI-... key into "
        "the widget at the top of the notebook, then re-run this cell."
    )
if not api_key.startswith("RGAPI-"):
    raise ValueError("The key does not look like a Riot API key (expected 'RGAPI-...').")

# The notebook's REST context: host + a short-lived session token.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
host = "https://" + ctx.tags().get("browserHostName").get()
headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}

# --- Create the scope (tolerate "already exists") ---
resp = requests.post(
    f"{host}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": SCOPE, "scope_backend_type": "DATABRICKS"},
)
if resp.status_code == 200:
    print(f"Scope '{SCOPE}' created.")
elif "RESOURCE_ALREADY_EXISTS" in resp.text:
    print(f"Scope '{SCOPE}' already exists — reusing it.")
else:
    raise RuntimeError(f"create-scope failed: {resp.status_code} {resp.text}")

# --- Store the key (overwrites if already present) ---
resp = requests.post(
    f"{host}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": SCOPE, "key": SECRET_KEY, "string_value": api_key},
)
if resp.status_code != 200:
    raise RuntimeError(f"put-secret failed: {resp.status_code} {resp.text}")
print(f"Secret '{SECRET_KEY}' stored in scope '{SCOPE}'.")

# Drop the local reference to the key as soon as it has been sent.
del api_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Verify
# MAGIC
# MAGIC Confirms the scope and key exist. `dbutils.secrets.get` returns the
# MAGIC value but Databricks **redacts it** in any output — so printing its
# MAGIC length is the safe way to prove it is non-empty.

# COMMAND ----------

scopes = [s.name for s in dbutils.secrets.listScopes()]  # noqa: F821
assert SCOPE in scopes, f"Scope '{SCOPE}' not found after creation"

keys = [k.key for k in dbutils.secrets.list(SCOPE)]  # noqa: F821
assert SECRET_KEY in keys, f"Key '{SECRET_KEY}' not found in scope '{SCOPE}'"

# Reading the secret back; Databricks redacts the value in output.
fetched = dbutils.secrets.get(scope=SCOPE, key=SECRET_KEY)  # noqa: F821
print(f"Scope '{SCOPE}' OK. Key '{SECRET_KEY}' OK. Stored value length: {len(fetched)}")
print("Setup complete — the Bronze ingestion notebooks can now read the key.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Clean up
# MAGIC
# MAGIC Clear the `riot_api_key` widget value now (so the key does not stay
# MAGIC visible in the notebook session). Re-run this notebook only when the
# MAGIC dev key expires and you need to store a fresh one.

# COMMAND ----------

dbutils.widgets.remove("riot_api_key")  # noqa: F821
print("Widget removed. You can detach this notebook.")
