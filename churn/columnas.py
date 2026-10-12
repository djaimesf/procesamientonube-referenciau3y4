"""Listas de columnas del caso, sin pyspark: las usan silver.py (U3) y revision.py (U4)."""

# Columnas comunes a los dos archivos de origen (info churn, entrenamiento
# IBM, y lotes del crm). No incluye "Churn" ni "fecha_lote": esas son las
# columnas que distinguen a cada fuente.
COLUMNAS_BASE = [
    "customerID",
    "gender",
    "SeniorCitizen",
    "Partner",
    "Dependents",
    "tenure",
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "Contract",
    "PaperlessBilling",
    "PaymentMethod",
    "MonthlyCharges",
    "TotalCharges",
]

# Info churn: entrenamiento IBM (21 columnas, incluye "Churn").
COLUMNAS_INFO_CHURN = COLUMNAS_BASE + ["Churn"]

# CRM: lotes semanales (21 columnas, incluye "fecha_lote"). En el archivo
# real "fecha_lote" esta en la posicion 2, no al final; por eso leer_csv
# empareja por NOMBRE y no por posicion.
COLUMNAS_CRM = COLUMNAS_BASE + ["fecha_lote"]
