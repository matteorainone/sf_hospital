CREATE OR REPLACE SCHEMA CORE;

--CREAZIONE TABELLA PER DATI ANAGRAFICI DEI PAZIENTI
CREATE OR REPLACE TABLE CORE.PATIENTS (
    PATIENT_ID STRING PRIMARY KEY,
    NOME STRING,
    COGNOME STRING,
    DATA_NASCITA DATE,
    SESSO STRING,
    CF string
)
;

/*DEFINIZIONE DI UN SISTEMA PER POPOLARE I DATI DEI PAZIENTI PARTENDO
DAI DATI PRESENTI SULLE TABELLE DI STAGING */
--CREAZIONE DI UNO STREAM PER MONITORARE L'AGGIUNTA DI CARTELLE MEDICHE
CREATE OR REPLACE STREAM STAGING.EHR_LANDING_STREAM_PATIENTS_DATA
ON TABLE STAGING.EHR_LANDING
--APPEND_ONLY = TRUE
;
--CREAZIONE DI UN TASK PER IL POPOLAMENTO DELLA TABELLA PATIENTS
CREATE OR REPLACE TASK CORE.INSERT_PATIENTS_ANAG
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('STAGING.EHR_LANDING_STREAM_PATIENTS_DATA')
AS
INSERT INTO CORE.PATIENTS (
    PATIENT_ID,
    NOME,
    COGNOME,
    DATA_NASCITA,
    SESSO,
    CF
    )
SELECT
    raw_json:patient_id::STRING as PATIENT_ID,
    SPLIT_PART(raw_json:name::STRING, ' ', 1) AS NOME,
    SPLIT_PART(raw_json:name::STRING, ' ', 2) AS COGNOME,
    raw_json:date_of_birth::DATE AS DATA_NASCITA,
    raw_json:gender::STRING AS SESSO,
    pgd.cf
FROM STAGING.EHR_LANDING_STREAM_PATIENTS_DATA ehr
LEFT JOIN STAGING.PATIENTS_GENERAL_DATA pgd
ON ehr.raw_json:patient_id::STRING = pgd.id_paziente
;

ALTER TASK CORE.INSERT_PATIENTS_ANAG RESUME;

/* CREAZIONE DI UNA TABELLA PER LE AMMISSIONI*/
CREATE OR REPLACE TABLE CORE.HOSPITAL_ADMISSIONS (
    PATIENT_ID STRING PRIMARY KEY,
    admission_id string UNIQUE,
    admission_date DATE,
    discharge_date DATE,
    department STRING,
    admitting_doctor STRING,
    diagnosis STRING,
    outcome STRING,
    COSTO_TOTALE NUMBER(10,2),
    note STRING
);
--STREAM PER LE AMMISSIONI
CREATE OR REPLACE STREAM STAGING.EHR_LANDING_STREAM_ADMISSIONS
ON TABLE STAGING.EHR_LANDING
--APPEND_ONLY = TRUE
;

--task per prendere i dati delle ammissioni
CREATE OR REPLACE TASK CORE.INSERT_ADMISSIONS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.EHR_LANDING_STREAM_ADMISSIONS')
AS
INSERT INTO CORE.HOSPITAL_ADMISSIONS (
    PATIENT_ID, admission_id, admission_date, discharge_date, department, admitting_doctor, diagnosis, outcome, costo_totale, note
)
WITH admissions_flat AS (
    SELECT
        ehr.raw_json:patient_id::STRING AS patient_id,
        admission.value:admission_id::STRING AS admission_id,
        admission.value:admission_date::DATE AS admission_date,
        admission.value:discharge_date::DATE AS discharge_date,
        admission.value:department::STRING AS department,
        admission.value:admitting_doctor::STRING AS admitting_doctor,
        admission.value:diagnosis::STRING AS diagnosis,
        admission.value:outcomes::STRING AS outcome,
        ehr.raw_json:notes::STRING AS note
    FROM STAGING.EHR_LANDING_STREAM_ADMISSIONS ehr,
         LATERAL FLATTEN(input => ehr.raw_json:admissions) AS admission
)
SELECT
    af.patient_id,
    af.admission_id,
    af.admission_date,
    af.discharge_date,
    af.department,
    af.admitting_doctor,
    af.diagnosis,
    af.outcome,
    pdg.costo_totale,
    af.note
FROM admissions_flat af
LEFT JOIN STAGING.patients_general_data pdg
    ON pdg.id_paziente = af.patient_id
   AND pdg.data_accesso = af.admission_date
   AND pdg.data_dimissione = af.discharge_date
;
ALTER TASK CORE.INSERT_ADMISSIONS RESUME;

/* CREAZIONE DI UNA TABELLA PER I SEGNI VITALI DEI PAZIENTI*/
CREATE OR REPLACE TABLE core.vital_signs_consolidated (
    patient_id STRING,
    date DATE,
    heart_rate INT,
    spo2 INT,
    temperature FLOAT,
    source STRING, -- 'EHR' o 'IOT'
    source_ts TIMESTAMP_TZ -- data del dato
);

--CREAZIONE DELLO STREAM PER I SEGNI VITALI
--STREAM PER LE AMMISSIONI
CREATE OR REPLACE STREAM STAGING.EHR_LANDING_STREAM_VITALS
ON TABLE STAGING.EHR_LANDING
--APPEND_ONLY = TRUE
;

/*CREAZIONE DEL TASK PER IL POPOLAMENTO DELLA TABELLA
CONTIENE UNA LOGICA DI VERIFICA DI COERENZA DEL DATO,
UTILIZZANDO SEMPRE IL SEGNALE PIU' AGGIORNATO TRA LA CARTELLA MEDICA E 
LA SENSORISTICA IOT */
CREATE OR REPLACE TASK CORE.INSERT_VITAL_SIGNS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STAGING.EHR_LANDING_STREAM_VITALS')
AS
MERGE INTO CORE.vital_signs_consolidated t
USING (
WITH EHR_DATA AS (
    SELECT
        raw_json:patient_id::STRING AS patient_id,
        vitals.value:date::DATE AS date,
        vitals.value:heart_rate::INT AS heart_rate,
        vitals.value:spo2::INT AS spo2,
        vitals.value:temp_c::FLOAT AS temperature,
        'EHR' AS source,
        update_timestamp::TIMESTAMP_NTZ AS source_ts
    FROM staging.EHR_LANDING_STREAM_VITALS,
         LATERAL FLATTEN(input => raw_json:admissions) AS ADM,
         LATERAL FLATTEN(input => ADM.value:vital_signs) AS vitals
),
IOT_DATA AS (
    SELECT 
        patient_code AS patient_id,
        CAST(timestamp AS DATE) AS date,
        heart_rate_bpm AS heart_rate,
        spo2_percent AS spo2,
        temperature_celsius AS temperature,
        'IOT' AS source,
        timestamp::TIMESTAMP_NTZ AS source_ts
    FROM staging.patients_iot_vitals
),
UNIFIED AS (
    SELECT * FROM EHR_DATA
    UNION ALL
    SELECT * FROM IOT_DATA
),
RANKED AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY patient_id, date ORDER BY source_ts DESC) AS rn
    FROM UNIFIED
)
SELECT *
FROM RANKED
WHERE rn = 1
) s
ON T.PATIENT_ID = S.PATIENT_ID AND T.DATE = S.DATE
WHEN MATCHED THEN UPDATE SET
    t.heart_rate = s.heart_rate,
    t.spo2 = s.spo2,
    t.temperature = s.temperature,
    t.source = s.source,
    t.source_ts = s.source_ts
WHEN NOT MATCHED THEN INSERT (
    patient_id, date, heart_rate, spo2, temperature, source, source_ts
) VALUES (
    s.patient_id, s.date, s.heart_rate, s.spo2, s.temperature, s.source, s.source_ts
)
;
ALTER TASK CORE.INSERT_VITAL_SIGNS RESUME;

/* creazione tabella per le prescrizioni*/
create or replace table core.prescriptions (
    ID BIGINT AUTOINCREMENT PRIMARY KEY,
    admission_id STRING REFERENCES CORE.HOSPITAL_ADMISSIONS(admission_id),
    DRUG STRING,
    DOSE STRING,
    ROUTE STRING,
    FREQUENCY STRING,
    START_DATE DATE,
    END_DATE DATE
)
;

--CREAZIONE DELLO STREAM PER LE PRESCRIZIONI
CREATE OR REPLACE STREAM STAGING.EHR_LANDING_STREAM_PRESCRIPTIONS
ON TABLE STAGING.EHR_LANDING
--APPEND_ONLY = TRUE
;
--creazione task per poplare le prescrizioni
CREATE OR REPLACE TASK CORE.task_insert_prescriptions
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('staging.EHR_LANDING_STREAM_PRESCRIPTIONS')
AS
INSERT INTO core.prescriptions (
    admission_id, drug, dose, route, frequency, start_date, end_date
)
SELECT
    adm.value:admission_id::STRING AS admission_id,
    p.value:drug::STRING AS drug,
    p.value:dose::STRING AS dose,
    p.value:route::STRING AS route,
    p.value:frequency::STRING AS frequency,
    p.value:start::DATE AS start_date,
    p.value:end::DATE AS end_date
FROM staging.EHR_LANDING_STREAM_PRESCRIPTIONS,
     LATERAL FLATTEN(input => raw_json:admissions) AS adm,
     LATERAL FLATTEN(input => adm.value:prescriptions) AS p
;
ALTER TASK CORE.task_insert_prescriptions RESUME;

--TERAPIE
CREATE OR REPLACE TABLE CORE.PROCEDURES (
    ID BIGINT AUTOINCREMENT PRIMARY KEY,
    admission_id STRING REFERENCES CORE.HOSPITAL_ADMISSIONS(admission_id),
    PROCEDURE_NAME STRING
)
;

--STREAM PER TERAPIE
CREATE OR REPLACE STREAM STAGING.EHR_LANDING_STREAM_PROCEDURES
ON TABLE STAGING.EHR_LANDING
--APPEND_ONLY = TRUE
;
--CREAZIONE TASK PER TERAPIE
CREATE OR REPLACE TASK CORE.INSERT_PROCEDURES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('staging.EHR_LANDING_STREAM_PROCEDURES')
AS
INSERT INTO CORE.PROCEDURES (
    ADMISSION_ID, PROCEDURE_NAME
)
SELECT 
    adm.value:admission_id::STRING AS admission_id,
    proc.value::STRING AS procedure_name
FROM staging.EHR_LANDING_STREAM_PROCEDURES,
     LATERAL FLATTEN(input => raw_json:admissions) AS adm,
     LATERAL FLATTEN(input => adm.value:procedures) AS proc
;

ALTER TASK CORE.INSERT_PROCEDURES RESUME;
--TABELLA PER FATTURE
CREATE TABLE core.invoices (
    invoice_id STRING PRIMARY KEY,
    data_fattura DATE,
    reparto STRING,
    tipo_fattura STRING,
    paziente_id STRING REFERENCES core.patients(patient_id),
    nome_paziente STRING,
    descrizione_prestazione STRING,
    fornitore STRING,
    descrizione_fornitura STRING,
    importo NUMBER(10,2),
    stato STRING
)
;
--CREAZIONE DI UNO STREAM PER LE FATTURE
CREATE OR REPLACE STREAM STAGING.INVOICES_STREAM
ON TABLE STAGING.FATTURE_REPARTI
;

--TASK PER CARICARE I DATI DELLE FATTURE
CREATE OR REPLACE TASK CORE.INSERT_INVOICES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('staging.INVOICES_STREAM')
AS
INSERT INTO CORE.INVOICES (
    INVOICE_ID, DATA_FATTURA, REPARTO, TIPO_FATTURA, PAZIENTE_ID,
    NOME_PAZIENTE, DESCRIZIONE_PRESTAZIONE, FORNITORE, DESCRIZIONE_FORNITURA,
    IMPORTO, STATO
)
SELECT INVOICE_ID, DATA_FATTURA, REPARTO, TIPO_FATTURA, PAZIENTE_ID,
    NOME_PAZIENTE, DESCRIZIONE_PRESTAZIONE, FORNITORE, DESCRIZIONE_FORNITURA,
    IMPORTO, STATO
FROM STAGING.INVOICES_STREAM
;
ALTER TASK CORE.INSERT_INVOICES RESUME;