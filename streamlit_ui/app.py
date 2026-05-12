# ============================================================
# 📦 IMPORTS
# ============================================================

import json
import random
import requests
import streamlit as st

# ============================================================
# ⚙️ CONFIG
# ============================================================

API_URL = "http://api-inference:8001"

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Diabetes Readmission",
    layout="wide"
)

st.title(
    "🏥 Diabetes Readmission Predictor"
)

# ============================================================
# LOAD HEALTH
# ============================================================

try:

    health_response = requests.get(
        f"{API_URL}/health",
        timeout=5
    )

    health_data = health_response.json()

except Exception as e:

    st.error(
        "❌ Could not connect to API"
    )

    st.stop()

# ============================================================
# API DOWN
# ============================================================

if not health_data["model_loaded"]:

    st.warning(
        "⚠️ No champion model loaded yet"
    )

    st.stop()

# ============================================================
# MODEL INFO
# ============================================================

model_info = requests.get(
    f"{API_URL}/model-info"
).json()

st.success(
    f"""
    Model: {model_info["model_name"]}

    Version: {model_info["model_version"]}

    Alias: {model_info["model_alias"]}
    """
)

# ============================================================
# LOAD FEATURE METADATA
# ============================================================

metadata_response = requests.get(
    f"{API_URL}/feature-metadata"
)

metadata = metadata_response.json()

features = metadata["features"]

# ============================================================
# EXAMPLE BUTTON
# ============================================================

if "example_payload" not in st.session_state:

    st.session_state.example_payload = {}

def generate_example_payload():

    payload = {}

    for feature_name, feature_meta in (
        features.items()
    ):

        # ----------------------------------------------------
        # CATEGORICAL
        # ----------------------------------------------------

        if feature_meta["type"] == "categorical":

            payload[feature_name] = random.choice(
                feature_meta["values"]
            )

        # ----------------------------------------------------
        # NUMERIC
        # ----------------------------------------------------

        else:

            min_value = feature_meta["min"]
            max_value = feature_meta["max"]

            if (
                min_value is None
                or max_value is None
            ):

                payload[feature_name] = 0

            else:

                payload[feature_name] = (
                    (min_value + max_value) / 2
                )

    st.session_state.example_payload = payload

st.button(
    "🎲 Load Example Values",
    on_click=generate_example_payload
)

# ============================================================
# FORM
# ============================================================

payload = {}

with st.form("prediction_form"):

    st.subheader(
        "Patient Features"
    )

    columns = st.columns(3)

    feature_names = list(
        features.keys()
    )

    for idx, feature_name in enumerate(
        feature_names
    ):

        feature_meta = features[
            feature_name
        ]

        default_value = (
            st.session_state
            .example_payload
            .get(feature_name)
        )

        with columns[idx % 3]:

            # ------------------------------------------------
            # CATEGORICAL
            # ------------------------------------------------

            if (
                feature_meta["type"]
                == "categorical"
            ):

                values = feature_meta["values"]

                if (
                    default_value
                    in values
                ):

                    default_index = values.index(
                        default_value
                    )

                else:

                    default_index = 0

                payload[feature_name] = (
                    st.selectbox(
                        feature_name,
                        values,
                        index=default_index
                    )
                )

            # ------------------------------------------------
            # NUMERIC
            # ------------------------------------------------

            else:

                min_value = feature_meta["min"]
                max_value = feature_meta["max"]

                if min_value is None:
                    min_value = 0.0

                if max_value is None:
                    max_value = 100.0

                if default_value is None:

                    default_value = (
                        min_value + max_value
                    ) / 2

                payload[feature_name] = (
                    st.number_input(
                        feature_name,
                        value=float(default_value),
                        min_value=float(min_value),
                        max_value=float(max_value)
                    )
                )

    submitted = st.form_submit_button(
        "🔮 Predict"
    )

# ============================================================
# PREDICT
# ============================================================

if submitted:

    with st.spinner(
        "Running inference..."
    ):

        try:

            response = requests.post(
                f"{API_URL}/predict",
                json=payload,
                timeout=30
            )

            # ------------------------------------------------
            # ERROR
            # ------------------------------------------------

            if response.status_code != 200:

                st.error(
                    response.text
                )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            else:

                result = response.json()

                st.success(
                    "✅ Prediction completed"
                )

                col1, col2 = st.columns(2)

                with col1:

                    st.metric(
                        "Prediction",
                        result["prediction"]
                    )

                    st.metric(
                        "Probability",
                        round(
                            result["probability"],
                            4
                        )
                    )

                with col2:

                    st.metric(
                        "Model Version",
                        result["model_version"]
                    )

                    st.metric(
                        "Latency (ms)",
                        result[
                            "processing_time_ms"
                        ]
                    )

                st.subheader(
                    "Raw Response"
                )

                st.json(result)

        except Exception as e:

            st.error(str(e))