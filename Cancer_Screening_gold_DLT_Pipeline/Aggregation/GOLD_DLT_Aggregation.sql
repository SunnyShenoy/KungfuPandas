-- Create Aggregated Patient Table
CREATE OR REPLACE LIVE TABLE patient_details_agg
COMMENT "Gold-layer table combining all fields from patient_details and patient_demographics, with fresh snapshot date"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  -- All fields from patient_details
  d.Patient_ID AS Patient_ID,
  CAST(d.Patient_Name AS STRUCT<
    Title: STRING,
    First_Name: STRING,
    Surname: STRING
  >) AS Patient_Name,

  CAST(d.Patient_Address AS STRUCT<
    Address: STRING,
    Suburb: STRING
  >) AS Patient_Address,

  d.DOB AS DOB,
  d.Mobile_Phone AS Mobile_Phone,
  d.Home_Phone AS Home_Phone,
  d.Work_Phone AS Work_Phone,
  d.Age AS Age,
  d.Age_Group AS Age_Group,

  -- All fields from patient_demographics
  g.gender AS Gender,
  g.marital_status AS Marital_Status,
  g.employment_status AS Employment_Status,
  g.education_level AS Education_Level,
  g.ethnicity AS Ethnicity,
  CAST(g.income_profile AS STRUCT<
    income: FLOAT,
    income_group: STRING,
    insurance: STRING
  >) AS Income_Profile,

  CAST(g.socioeconomic_info AS STRUCT<
    education: STRING,
    employment: STRING,
    income: FLOAT,
    income_group: STRING
  >) AS Socioeconomic_Info,

  CURRENT_DATE() AS dataset_snapshot_date,
  d.Current_Environment AS Current_Environment

FROM medical_research_recalls.silver.patient_details d
LEFT JOIN medical_research_recalls.silver.patient_demographics g
  ON d.Patient_ID = g.Patient_ID;

-- Create Aggregated Risk Score Table
CREATE OR REPLACE LIVE TABLE patient_risk_profile_agg
COMMENT "Gold-layer table combining cancer risk factors and scoring for each patient"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  -- Identity & Demographics
  f.id AS patient_id,
  f.age AS age,
  f.gender AS gender,

  -- Binary Risk Factors
  f.smoking AS smoking,
  f.alcohol_consumption AS alcohol_consumption,
  f.family_history AS family_history,
  f.poor_diet AS poor_diet,
  f.obesity AS obesity,
  f.sedentary_lifestyle AS sedentary_lifestyle,
  f.exposure_to_carcinogens AS exposure_to_carcinogens,
  f.chronic_inflammation AS chronic_inflammation,
  f.hpv_infection AS hpv_infection,
  f.sun_exposure AS sun_exposure,
  f.air_pollution AS air_pollution,
  f.radiation_exposure AS radiation_exposure,
  f.chemical_exposure AS chemical_exposure,
  f.immunosuppression AS immunosuppression,
  f.night_shift_work AS night_shift_work,
  f.risk_factor_count AS risk_factor_count,

  -- Explicit STRUCT cast to support DLT/Unity Catalog compatibility
  CAST(f.risk_profile AS STRUCT<
    smoking: INT,
    alcohol_consumption: INT,
    family_history: INT,
    poor_diet: INT,
    obesity: INT,
    sedentary_lifestyle: INT,
    exposure_to_carcinogens: INT,
    chronic_inflammation: INT,
    hpv_infection: INT,
    sun_exposure: INT,
    air_pollution: INT,
    radiation_exposure: INT,
    chemical_exposure: INT,
    immunosuppression: INT,
    night_shift_work: INT
  >) AS risk_profile,
  
  -- Risk Factor Scores
  s.smoking_score AS smoking_score,
  s.alcohol_consumption_score AS alcohol_consumption_score,
  s.family_history_score AS family_history_score,
  s.poor_diet_score AS poor_diet_score,
  s.obesity_score AS obesity_score,
  s.sedentary_lifestyle_score AS sedentary_lifestyle_score,
  s.exposure_to_carcinogens_score AS exposure_to_carcinogens_score,
  s.chronic_inflammation_score AS chronic_inflammation_score,
  s.hpv_infection_score AS hpv_infection_score,
  s.sun_exposure_score AS sun_exposure_score,
  s.air_pollution_score AS air_pollution_score,
  s.radiation_exposure_score AS radiation_exposure_score,
  s.chemical_exposure_score AS chemical_exposure_score,
  s.immunosuppression_score AS immunosuppression_score,
  s.night_shift_work_score AS night_shift_work_score,
  s.total_risk_score AS total_risk_score,

  -- Cancer Probabilities
  s.lung_cancer AS lung_cancer,
  s.breast_cancer AS breast_cancer,
  s.colon_cancer AS colon_cancer,
  s.skin_cancer AS skin_cancer,
  s.cervical_cancer AS cervical_cancer,
  s.prostate_cancer AS prostate_cancer,
  s.leukemia AS leukemia,
  s.pancreatic_cancer AS pancreatic_cancer,
  s.avg_cancer_probability AS avg_cancer_probability,
  s.max_cancer_probability AS max_cancer_probability,

  -- Grouping Fields
  s.cancer_risk_group AS cancer_risk_group,
  s.risk_group AS score_based_risk_group,

  -- Gold-layer tracking fields
  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS current_environment

FROM medical_research_recalls.silver.patient_risk_factors f
LEFT JOIN medical_research_recalls.silver.patient_risk_scores s
  ON f.id = s.id;

-- Cancer Detection

CREATE OR REPLACE LIVE TABLE patient_cancer_detection_metrics
COMMENT "Gold-layer detection metrics with refreshed snapshot and environment"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  patient_id,
  detection_date,
  detection_method,
  cancer_type,
  cancer_probability,
  detection_details,  -- named_struct already created in Silver

  -- Gold-specific metadata
  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS Current_Environment

FROM medical_research_recalls.silver.patient_cancer_detection;


-- Suburb Aggregation
CREATE OR REPLACE LIVE TABLE suburb_patient_summary
COMMENT "Gold-layer table aggregating patient count per suburb"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  `Patient_Address`.`Suburb` AS Suburb,
  COUNT(`Patient_ID`) AS Total_Patients,
  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS current_environment
FROM
  medical_research_recalls.gold.patient_details_agg
WHERE
  `Patient_Address`.`Suburb` IS NOT NULL
GROUP BY
  `Patient_Address`.`Suburb`;


-- Insurance Income Aggregation
CREATE OR REPLACE LIVE TABLE insurance_income_summary
COMMENT "Gold-layer table showing patient count by insurance type and income group"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  `income_profile`.`insurance` AS insurance,
  `income_profile`.`income_group` AS income_group,
  COUNT(DISTINCT `Patient_ID`) AS patient_count,
  CURRENT_DATE() AS dataset_snapshot_date,
  'Prod' AS current_environment
FROM
  medical_research_recalls.gold.patient_details_agg
GROUP BY
  `income_profile`.`insurance`,
  `income_profile`.`income_group`;