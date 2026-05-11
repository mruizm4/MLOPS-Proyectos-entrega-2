from locust import HttpUser, task, between

# ============================================================
# FIXED PAYLOAD
# ============================================================

PAYLOAD = {
  "race": "Other",
  "gender": "Female",
  "age": "[70-80)",
  "weight": "[75-100)",
  "admission_type_id": "Elective",
  "discharge_disposition_id": "Discharged/transferred to another rehab fac including rehab units of a hospital .",
  "admission_source_id": "Physician Referral",
  "time_in_hospital": 4,
  "payer_code": "HM",
  "medical_specialty": "Surgery-Neuro",
  "num_lab_procedures": 79,
  "num_procedures": 5,
  "num_medications": 45,
  "number_outpatient": 0,
  "number_emergency": 0,
  "number_inpatient": 6,
  "diag_1": "724",
  "diag_2": "396",
  "diag_3": "281",
  "number_diagnoses": 4,
  "max_glu_serum": "Norm",
  "a1cresult": ">7",
  "metformin": "Steady",
  "repaglinide": "Steady",
  "nateglinide": "Up",
  "chlorpropamide": "No",
  "glimepiride": "No",
  "acetohexamide": "No",
  "glipizide": "Up",
  "glyburide": "Steady",
  "tolbutamide": "No",
  "pioglitazone": "No",
  "rosiglitazone": "No",
  "acarbose": "Steady",
  "miglitol": "No",
  "troglitazone": "No",
  "tolazamide": "No",
  "examide": "No",
  "citoglipton": "No",
  "insulin": "No",
  "glyburide_metformin": "Steady",
  "glipizide_metformin": "No",
  "glimepiride_pioglitazone": "No",
  "metformin_rosiglitazone": "No",
  "metformin_pioglitazone": "No",
  "change": "No",
  "diabetesmed": "No"
}

# ============================================================
# LOCUST USER
# ============================================================

class UsuarioDeCarga(HttpUser):

    wait_time = between(1, 2.5)

    # --------------------------------------------------------
    # HEALTH
    # --------------------------------------------------------

    @task(1)
    def health(self):

        self.client.get("/health")

    # --------------------------------------------------------
    # MODEL INFO
    # --------------------------------------------------------

    @task(1)
    def model_info(self):

        self.client.get("/model-info")

    # --------------------------------------------------------
    # PREDICT
    # --------------------------------------------------------

    @task(5)
    def hacer_inferencia(self):

        response = self.client.post(
            "/predict",
            json=PAYLOAD
        )

        if response.status_code != 200:

            print(
                "❌ Error en inferencia:"
            )

            print(response.text)

        else:

            result = response.json()

            print(
                "✅ Prediction:",
                result["prediction"],
                "| Prob:",
                result["probability"]
            )