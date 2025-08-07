
# 🏥 Modello Dati Ospedaliero – Snowflake

## 📘 Descrizione generale
Questo progetto simula un sistema informativo ospedaliero completo, basato su **Snowflake**.  
Il modello copre flussi **clinici**, **gestionali**, **amministrativi** e **IoT**, consentendo:
- Ingestion automatizzata
- Trasformazione dei dati
- Analisi e reportistica
- Controllo degli accessi e data masking

---

## 📁 Componenti principali

### 1. STAGING Area (`STAGING`)
Contiene dati grezzi provenienti da:
- **Fonti cliniche** (EHR, IoT): JSON
- **Fonti gestionali** (ERP): CSV

**Strutture principali:**
- `EHR_LANDING`: Cartelle cliniche elettroniche (JSON)
- `PATIENTS_GENERAL_DATA`: Info gestionali pazienti
- `IOT_VITALS_RAW`: Dati da sensori IoT
- `FATTURE_REPARTI`: Fatture ospedaliere

**Automazioni:**
- `PIPE` per ingestion automatica da S3
- `STREAM` + `TASK` per trasformazione e caricamento

---

### 2. CORE Data Model (`CORE`)
Contiene dati normalizzati, integrati e pronti per analisi:

| Tabella                 | Descrizione |
|-------------------------|-------------|
| `PATIENTS`              | Dati anagrafici |
| `HOSPITAL_ADMISSIONS`   | Ricoveri ospedalieri |
| `VITAL_SIGNS_CONSOLIDATED` | Parametri vitali da EHR/IoT |
| `PRESCRIPTIONS`         | Prescrizioni terapeutiche |
| `PROCEDURES`            | Procedure eseguite |
| `INVOICES`              | Fatturazione pazienti/reparti |

---

### 3. Business Intelligence Views
Viste analitiche per:

- **Clinica**: `VW_LAST_VITAL_SIGNS`, `VW_PRESCRIPTIONS_DETAIL`
- **Economico/Amministrativa**: `VW_REPARTO_INVOICES_SUMMARY`, `VW_PATIENT_BILLING_SUMMARY`
- **Gestione e ricerca**: `VW_AVG_LENGTH_OF_STAY`, `VW_OUTCOME_STATS`, `VW_DASHBOARD_HOSPITAL_OVERVIEW`

---

### 4. Sicurezza e Accessi (`gestione_ruoli_permessi.sql`)
Gestione a ruoli:

| Ruolo           | Accesso                                  |
|------------------|-------------------------------------------|
| `ROLE_ADMIN`     | Accesso completo                         |
| `ROLE_DOCTOR`    | Dati clinici + identificativi            |
| `ROLE_NURSE`     | Anagrafica e parametri vitali            |
| `ROLE_BILLING`   | Fatture + costi                          |
| `ROLE_ANALYST`   | Viste aggregate                          |
| `ROLE_RESEARCHER`| Ricoveri e anagrafica                    |

**Data masking** applicato a:
- Codice fiscale (`CF`)
- Nome/Cognome
- Costi (`COSTO_TOTALE`)

---

## ⚙️ Flusso ETL

1. **Ingestion** da file CSV/JSON via `PIPE`
2. **Parsing** in tabelle RAW (staging)
3. **STREAM/TASK** per elaborazione automatica
4. **Popolamento tabelle CORE**
5. **Analisi e BI** con viste dedicate

---

## 🧩 Modello ER

📎 [Visualizza ER Diagram](hospital_er_model.png)

---


## 📦 Requisiti

- Snowflake Account
- Storage AWS S3 (integrazione definita)
- Database `SNOWFLAKE_LEARNING_DB`

---

## ✍️ Autore

Progetto sviluppato per simulazione architettura DWH ospedaliera su Snowflake.
