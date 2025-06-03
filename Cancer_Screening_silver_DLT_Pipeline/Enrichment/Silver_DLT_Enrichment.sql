CREATE OR REPLACE LIVE TABLE patient_details
AS
SELECT
  cast(id as INT) AS Patient_ID, 
  
  named_struct(
     'Title', cast(title as STRING),
     'First_Name', cast(first_name as STRING),
     'Surname', cast(surname as STRING)
  ) AS Patient_Name,
  
  named_struct(
     'Address', address,  
     -- Suburb: Extract 3rd last part of address
     'Suburb', split(Address, ', ')[size(split(Address, ', ')) - 2]
  ) as Patient_Address,
  to_date(date_of_birth, 'dd/MM/yyyy') AS DOB,
  
  -- Get mobile phone as it is due to the way they're setup
  cast(mobile as STRING) as Mobile_Phone,

  -- Validate and filter home phone data
  CASE 
    WHEN LENGTH(regexp_replace(home_phone, '\\D', '')) >= 8 THEN cast(home_phone as STRING)
    ELSE NULL 
  END AS Home_Phone,

  -- Validate and filter work phone data
  CASE 
    WHEN LENGTH(regexp_replace(work_phone, '\\D', '')) >= 8 THEN cast(work_phone as STRING)
    ELSE NULL 
  END AS Work_Phone,

  -- Calculate age
  cast((DATEDIFF(CURRENT_DATE(), to_date(date_of_birth, 'dd/MM/yyyy')) / 365) AS INT) AS Age,

  -- Derive age group
  CASE 
    WHEN DATEDIFF(CURRENT_DATE(), to_date(date_of_birth, 'dd/MM/yyyy')) / 365 < 20 THEN '<20'
    WHEN DATEDIFF(CURRENT_DATE(), to_date(date_of_birth, 'dd/MM/yyyy')) / 365 < 40 THEN '20–40'
    WHEN DATEDIFF(CURRENT_DATE(), to_date(date_of_birth, 'dd/MM/yyyy')) / 365 < 60 THEN '40–60'
    WHEN DATEDIFF(CURRENT_DATE(), to_date(date_of_birth, 'dd/MM/yyyy')) / 365 < 80 THEN '60–80'
    ELSE '80+'
  END AS Age_Group,

  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS Current_Environment

FROM medical_research_recalls.bronze.kgf_patient_details
WHERE id IS NOT NULL;

-- Patient Demographics

CREATE OR REPLACE LIVE TABLE patient_demographics
COMMENT "Patient demographics enriched with income grouping and standard types"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(id AS INT) AS Patient_ID,
  cast(gender AS STRING) AS gender,
  marital_status,
  employment_status,
  education_level,
  ethnicity,
  
  -- Convert all primary language other to English
  CASE 
    WHEN primary_language = 'Other' THEN 'English'
    ELSE primary_language
  END AS primary_language,

  -- Income raw and grouped
  named_struct(
    'income', annual_income_aud,
    'income_group', CASE
      WHEN annual_income_aud < 30000 THEN 'Low'
      WHEN annual_income_aud < 70000 THEN 'Middle'
      WHEN annual_income_aud < 120000 THEN 'Upper-Middle'
      ELSE 
        'High'
    END,

    -- Insurance classification
    'insurance', CASE 
      WHEN insurance_type = 'Private' THEN 'Private'
      WHEN insurance_type = 'Public' THEN 'Government'
      ELSE 
        'Uninsured'
    END
  ) AS income_profile,

  named_struct(
    'education', education_level,
    'employment', employment_status,
    'income', annual_income_aud,
    'income_group', CASE
                      WHEN annual_income_aud < 30000 THEN 'Low'
                      WHEN annual_income_aud < 70000 THEN 'Middle'
                      WHEN annual_income_aud < 120000 THEN 'Upper-Middle'
                      ELSE 'High'
                    END
  ) AS socioeconomic_info,
  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS Current_Environment

FROM medical_research_recalls.bronze.kgf_patient_demographics
WHERE id IS NOT NULL;

-- Cancer Risk

CREATE OR REPLACE LIVE TABLE patient_risk_factors
COMMENT "Normalized patient cancer risk factors with computed enrichment fields"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(id AS INT) AS id,
  Age,
  gender,

  -- Explicit boolean checks with CAST to INT
  CAST(CASE WHEN smoking IS TRUE THEN 1 ELSE 0 END AS INT) AS smoking,
  CAST(CASE WHEN alcohol_consumption IS TRUE THEN 1 ELSE 0 END AS INT) AS alcohol_consumption,
  CAST(CASE WHEN family_history IS TRUE THEN 1 ELSE 0 END AS INT) AS family_history,
  CAST(CASE WHEN poor_diet IS TRUE THEN 1 ELSE 0 END AS INT) AS poor_diet,
  CAST(CASE WHEN obesity IS TRUE THEN 1 ELSE 0 END AS INT) AS obesity,
  CAST(CASE WHEN sedentary_lifestyle IS TRUE THEN 1 ELSE 0 END AS INT) AS sedentary_lifestyle,
  CAST(CASE WHEN exposure_to_carcinogens IS TRUE THEN 1 ELSE 0 END AS INT) AS exposure_to_carcinogens,
  CAST(CASE WHEN chronic_inflammation IS TRUE THEN 1 ELSE 0 END AS INT) AS chronic_inflammation,
  CAST(CASE WHEN hpv_infection IS TRUE THEN 1 ELSE 0 END AS INT) AS hpv_infection,
  CAST(CASE WHEN sun_exposure IS TRUE THEN 1 ELSE 0 END AS INT) AS sun_exposure,
  CAST(CASE WHEN air_pollution IS TRUE THEN 1 ELSE 0 END AS INT) AS air_pollution,
  CAST(CASE WHEN radiation_exposure IS TRUE THEN 1 ELSE 0 END AS INT) AS radiation_exposure,
  CAST(CASE WHEN chemical_exposure IS TRUE THEN 1 ELSE 0 END AS INT) AS chemical_exposure,
  CAST(CASE WHEN immunosuppression IS TRUE THEN 1 ELSE 0 END AS INT) AS immunosuppression,
  CAST(CASE WHEN night_shift_work IS TRUE THEN 1 ELSE 0 END AS INT) AS night_shift_work,

  -- Risk factor count
  (
    CAST(CASE WHEN smoking IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN alcohol_consumption IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN family_history IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN poor_diet IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN obesity IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN sedentary_lifestyle IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN exposure_to_carcinogens IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN chronic_inflammation IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN hpv_infection IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN sun_exposure IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN air_pollution IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN radiation_exposure IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN chemical_exposure IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN immunosuppression IS TRUE THEN 1 ELSE 0 END AS INT) +
    CAST(CASE WHEN night_shift_work IS TRUE THEN 1 ELSE 0 END AS INT)
  ) AS risk_factor_count,

  -- STRUCT of risk factors for ML/dashboards
  named_struct(
    'smoking', CAST(CASE WHEN smoking IS TRUE THEN 1 ELSE 0 END AS INT),
    'alcohol_consumption', CAST(CASE WHEN alcohol_consumption IS TRUE THEN 1 ELSE 0 END AS INT),
    'family_history', CAST(CASE WHEN family_history IS TRUE THEN 1 ELSE 0 END AS INT),
    'poor_diet', CAST(CASE WHEN poor_diet IS TRUE THEN 1 ELSE 0 END AS INT),
    'obesity', CAST(CASE WHEN obesity IS TRUE THEN 1 ELSE 0 END AS INT),
    'sedentary_lifestyle', CAST(CASE WHEN sedentary_lifestyle IS TRUE THEN 1 ELSE 0 END AS INT),
    'exposure_to_carcinogens', CAST(CASE WHEN exposure_to_carcinogens IS TRUE THEN 1 ELSE 0 END AS INT),
    'chronic_inflammation', CAST(CASE WHEN chronic_inflammation IS TRUE THEN 1 ELSE 0 END AS INT),
    'hpv_infection', CAST(CASE WHEN hpv_infection IS TRUE THEN 1 ELSE 0 END AS INT),
    'sun_exposure', CAST(CASE WHEN sun_exposure IS TRUE THEN 1 ELSE 0 END AS INT),
    'air_pollution', CAST(CASE WHEN air_pollution IS TRUE THEN 1 ELSE 0 END AS INT),
    'radiation_exposure', CAST(CASE WHEN radiation_exposure IS TRUE THEN 1 ELSE 0 END AS INT),
    'chemical_exposure', CAST(CASE WHEN chemical_exposure IS TRUE THEN 1 ELSE 0 END AS INT),
    'immunosuppression', CAST(CASE WHEN immunosuppression IS TRUE THEN 1 ELSE 0 END AS INT),
    'night_shift_work', CAST(CASE WHEN night_shift_work IS TRUE THEN 1 ELSE 0 END AS INT)
  ) AS risk_profile,

  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS Current_Environment

FROM medical_research_recalls.bronze.kgf_patient_cancer_risk
WHERE id IS NOT NULL;


-- Cancer Detection

CREATE OR REPLACE LIVE TABLE patient_cancer_detection
COMMENT "Detection records with enriched stage and method categories"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(patient_id AS INT),
  CAST(detection_date AS DATE),
  detection_method,
  cancer_type,
  cancer_probability,

  named_struct(    'stage_at_detection', stage_at_detection,
    'stage_int', CASE 
      WHEN stage_at_detection = 'I' THEN 1
      WHEN stage_at_detection = 'II' THEN 2
      WHEN stage_at_detection = 'III' THEN 3
      WHEN stage_at_detection = 'IV' THEN 4
      ELSE NULL
    END,
    'detection_type', CASE 
      WHEN detection_method LIKE '%screen%' THEN 'Proactive'
      WHEN detection_method LIKE '%routine%' THEN 'Proactive'
      WHEN detection_method LIKE '%symptom%' THEN 'Reactive'
      ELSE 'Other'
    END
  ) AS detection_details,
  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS Current_Environment
  
FROM bronze.kgf_patient_cancer_detection
WHERE patient_id IS NOT NULL;


-- Score Analytics

CREATE OR REPLACE LIVE TABLE patient_risk_scores
COMMENT "Normalized and enriched patient risk scores with rounded cancer probabilities"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(id AS INT) AS id,
  CAST(age AS INT) AS age,
  CAST(gender AS STRING) AS gender,

  -- Risk factor scores
  CAST(smoking_score AS INT) AS smoking_score,
  CAST(alcohol_consumption_score AS INT) AS alcohol_consumption_score,
  CAST(family_history_score AS INT) AS family_history_score,
  CAST(poor_diet_score AS INT) AS poor_diet_score,
  CAST(obesity_score AS INT) AS obesity_score,
  CAST(sedentary_lifestyle_score AS INT) AS sedentary_lifestyle_score,
  CAST(exposure_to_carcinogens_score AS INT) AS exposure_to_carcinogens_score,
  CAST(chronic_inflammation_score AS INT) AS chronic_inflammation_score,
  CAST(hpv_infection_score AS INT) AS hpv_infection_score,
  CAST(sun_exposure_score AS INT) AS sun_exposure_score,
  CAST(air_pollution_score AS INT) AS air_pollution_score,
  CAST(radiation_exposure_score AS INT) AS radiation_exposure_score,
  CAST(chemical_exposure_score AS INT) AS chemical_exposure_score,
  CAST(immunosuppression_score AS INT) AS immunosuppression_score,
  CAST(night_shift_work_score AS INT) AS night_shift_work_score,

  CAST(total_risk_score AS INT) AS total_risk_score,

  -- Rounded cancer probabilities (scaled ×10)
  ROUND(CAST(lung_cancer AS FLOAT), 2) AS lung_cancer,
  ROUND(CAST(breast_cancer AS FLOAT), 2) AS breast_cancer,
  ROUND(CAST(colon_cancer AS FLOAT), 2) AS colon_cancer,
  ROUND(CAST(skin_cancer AS FLOAT), 2) AS skin_cancer,
  ROUND(CAST(cervical_cancer AS FLOAT), 2) AS cervical_cancer,
  ROUND(CAST(prostate_cancer AS FLOAT), 2) AS prostate_cancer,
  ROUND(CAST(leukemia AS FLOAT), 2) AS leukemia,
  ROUND(CAST(pancreatic_cancer AS FLOAT), 2) AS pancreatic_cancer,

  -- Derived average cancer probability (FLOAT, already scaled and rounded)
  ROUND((
    lung_cancer +
    breast_cancer +
    colon_cancer +
    skin_cancer +
    cervical_cancer +
    prostate_cancer +
    leukemia +
    pancreatic_cancer
  ) / 8.0, 2) AS avg_cancer_probability,

  -- Max cancer probability (for grouping)
  ROUND(GREATEST(
    lung_cancer,
    breast_cancer,
    colon_cancer,
    skin_cancer,
    cervical_cancer,
    prostate_cancer,
    leukemia,
    pancreatic_cancer
  ), 2) AS max_cancer_probability,

  -- Group patients based on highest cancer probability
  CASE
    WHEN GREATEST(
      lung_cancer * 100,
      breast_cancer * 100,
      colon_cancer * 100,
      skin_cancer * 100,
      cervical_cancer * 100,
      prostate_cancer * 100,
      leukemia * 100,
      pancreatic_cancer * 100
    ) < 15 THEN 'Low'
    WHEN GREATEST(
      lung_cancer * 100,
      breast_cancer * 100,
      colon_cancer * 100,
      skin_cancer * 100,
      cervical_cancer * 100,
      prostate_cancer * 100,
      leukemia * 100,
      pancreatic_cancer * 100
    ) BETWEEN 15 AND 39 THEN 'Moderate'
    WHEN GREATEST(
      lung_cancer * 100,
      breast_cancer * 100,
      colon_cancer * 100,
      skin_cancer * 100,
      cervical_cancer * 100,
      prostate_cancer * 100,
      leukemia * 100,
      pancreatic_cancer * 100
    ) BETWEEN 40 AND 59 THEN 'High'
    ELSE 'Critical'
  END AS cancer_risk_group,

  -- Risk group enrichment
  CASE 
    WHEN total_risk_score < 20 THEN 'Low'
    WHEN total_risk_score BETWEEN 20 AND 49 THEN 'Moderate'
    WHEN total_risk_score BETWEEN 50 AND 74 THEN 'High'
    ELSE 'Critical'
  END AS risk_group,

  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS current_environment

FROM medical_research_recalls.bronze.kgf_patient_risk_scores
WHERE id IS NOT NULL;

