"""Tests de la revision del lote CRM (fase roja de TDD). Sin Spark."""
import csv
import json

import pydantic
import pytest

from churn.columnas import COLUMNAS_CRM
from churn.revision import (
    ClienteLote,
    LoteBloqueado,
    revisar_encabezados,
    revisar_filas,
    revisar_lote,
    reglas_de_fila,
)

FECHA = "2026-09-07"
CLAVES_CUARENTENA = {
    "fecha_error", "unidad_etapa", "regla", "severidad",
    "archivo_origen", "customerID", "id_ejecucion", "registro_original",
}


def fila_valida(**cambios):
    """Fila completa y valida con las 21 columnas; cambios la modifican."""
    fila = {
        "customerID": "0001-AAAAA", "gender": "Female", "SeniorCitizen": "0",
        "Partner": "Yes", "Dependents": "No", "tenure": "12",
        "PhoneService": "Yes", "MultipleLines": "No",
        "InternetService": "DSL", "OnlineSecurity": "No",
        "OnlineBackup": "Yes", "DeviceProtection": "No",
        "TechSupport": "No", "StreamingTV": "No", "StreamingMovies": "No",
        "Contract": "Month-to-month", "PaperlessBilling": "Yes",
        "PaymentMethod": "Electronic check", "MonthlyCharges": "70.35",
        "TotalCharges": "844.2", "fecha_lote": FECHA,
    }
    fila.update(cambios)
    return fila


def escribir_csv(ruta, filas, encabezados=None):
    """Escribe un CSV de prueba; fecha_lote queda en la posicion 2."""
    if encabezados is None:
        resto = [c for c in COLUMNAS_CRM if c not in ("customerID", "fecha_lote")]
        encabezados = ["customerID", "fecha_lote"] + resto
    with open(ruta, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=encabezados, extrasaction="ignore")
        w.writeheader()
        w.writerows(filas)
    return str(ruta)


def leer_jsonl(ruta):
    with open(ruta, encoding="utf-8") as fh:
        return [json.loads(linea) for linea in fh if linea.strip()]


# --- Pieza 1: revisar_encabezados ---

def test_encabezados_completos_en_cualquier_orden():
    """Las 21 columnas, aunque vengan desordenadas, no generan avisos."""
    assert revisar_encabezados(list(reversed(COLUMNAS_CRM))) == []


def test_columna_extra_es_aviso():
    """Una columna nueva (BancoPago) solo advierte; no bloquea."""
    res = revisar_encabezados(COLUMNAS_CRM + ["BancoPago"])
    assert res == [
        {"regla": "columna_nueva", "severidad": "advierte", "columna": "BancoPago"}
    ]


def test_columna_faltante_bloquea():
    """Si falta TotalCharges, el lote se bloquea."""
    cols = [c for c in COLUMNAS_CRM if c != "TotalCharges"]
    assert revisar_encabezados(cols) == [
        {"regla": "columna_faltante", "severidad": "bloquea",
         "columna": "TotalCharges"}
    ]


def test_encabezado_con_espacios_y_mayusculas_cuenta_como_conocido():
    """" Tenure" es la columna tenure: no es nueva ni falta."""
    cols = [" Tenure" if c == "tenure" else c for c in COLUMNAS_CRM]
    assert revisar_encabezados(cols) == []


# --- Pieza 2: reglas_de_fila (ClienteLote) ---

def test_cliente_lote_es_modelo_pydantic():
    """Las reglas de fila se validan con un modelo pydantic."""
    assert issubclass(ClienteLote, pydantic.BaseModel)
    assert "Contract" in ClienteLote.model_fields


def test_fila_valida_sin_reglas():
    """Una fila correcta no incumple ninguna regla."""
    assert reglas_de_fila(fila_valida()) == []


def test_contrato_1_year_no_existe():
    """Contrato "1 year" no existe en el historico: la fila va a cuarentena."""
    assert reglas_de_fila(fila_valida(Contract="1 year")) == [
        "categoria_no_permitida_contract"
    ]


def test_metodo_pago_pse_no_existe():
    """El metodo de pago "PSE" no existe en el historico."""
    assert reglas_de_fila(fila_valida(PaymentMethod="PSE")) == [
        "categoria_no_permitida_paymentmethod"
    ]


def test_categoria_con_espacio_al_final_no_vale():
    """"One year " con espacio al final no es igual a "One year"."""
    assert reglas_de_fila(fila_valida(Contract="One year ")) == [
        "categoria_no_permitida_contract"
    ]


def test_senior_citizen_2_no_existe():
    """SeniorCitizen solo puede ser "0" o "1"."""
    assert reglas_de_fila(fila_valida(SeniorCitizen="2")) == [
        "categoria_no_permitida_seniorcitizen"
    ]


def test_dos_columnas_malas_dan_dos_reglas():
    """Si fallan dos columnas, se reportan las dos reglas (ordenadas)."""
    res = reglas_de_fila(fila_valida(Contract="1 year", PaymentMethod="PSE"))
    assert res == [
        "categoria_no_permitida_contract",
        "categoria_no_permitida_paymentmethod",
    ]


def test_tenure_negativo_es_imposible():
    """Una antiguedad de -3 meses es imposible."""
    assert reglas_de_fila(fila_valida(tenure="-3")) == ["valor_imposible_tenure"]


def test_tenure_con_decimal_es_imposible():
    """La antiguedad se cuenta en meses enteros: "72.0" no es valido."""
    assert reglas_de_fila(fila_valida(tenure="72.0")) == ["valor_imposible_tenure"]


def test_tenure_75_fuera_de_rango():
    """El historico llega hasta 72 meses: 75 esta fuera de rango."""
    assert reglas_de_fila(fila_valida(tenure="75")) == ["fuera_de_rango_tenure"]


@pytest.mark.parametrize("valor", ["abc", " "])
def test_tenure_no_numerico(valor):
    """Texto o vacio en tenure no es un numero."""
    assert reglas_de_fila(fila_valida(tenure=valor)) == ["no_numerico_tenure"]


@pytest.mark.parametrize("valor", ["18.25", "118.75"])
def test_monthly_charges_limites_son_validos(valor):
    """Los limites del historico (18.25 y 118.75) se aceptan."""
    assert reglas_de_fila(fila_valida(MonthlyCharges=valor)) == []


def test_monthly_charges_sobre_el_limite_fuera_de_rango():
    """118.76 supera el maximo historico de 118.75."""
    assert reglas_de_fila(fila_valida(MonthlyCharges="118.76")) == [
        "fuera_de_rango_monthlycharges"
    ]


def test_monthly_charges_negativo_es_imposible():
    """Un cargo mensual de -5 es imposible."""
    assert reglas_de_fila(fila_valida(MonthlyCharges="-5")) == [
        "valor_imposible_monthlycharges"
    ]


def test_total_charges_en_blanco_no_es_numerico():
    """TotalCharges con solo un espacio no es un numero."""
    assert reglas_de_fila(fila_valida(TotalCharges=" ")) == [
        "no_numerico_totalcharges"
    ]


def test_cliente_sin_id():
    """Sin customerID no se puede identificar al cliente."""
    assert reglas_de_fila(fila_valida(customerID="")) == ["cliente_sin_id"]


def test_cualquier_formato_de_id_es_valido():
    """No se exige un formato de ID: "XYZ" es aceptado."""
    assert reglas_de_fila(fila_valida(customerID="XYZ")) == []


# --- Pieza 3: revisar_filas ---

def revisar(filas):
    return revisar_filas(filas, FECHA, "clientes_2026-09-07.csv", "run_1")


def test_fecha_lote_distinta_va_a_cuarentena():
    """Si fecha_lote no es la del archivo, la fila va a cuarentena."""
    buenas, cuarentena = revisar([fila_valida(fecha_lote="2026-08-31")])
    assert buenas == []
    assert [r["regla"] for r in cuarentena] == ["fecha_lote_no_coincide"]


def test_fecha_lote_vacia_va_a_cuarentena():
    """Una fecha_lote vacia tampoco coincide con la del archivo."""
    _, cuarentena = revisar([fila_valida(fecha_lote="")])
    assert [r["regla"] for r in cuarentena] == ["fecha_lote_no_coincide"]


def test_duplicado_identico_se_descarta_sin_cuarentena():
    """Dos filas identicas: se queda una y no hay cuarentena."""
    fila = fila_valida()
    buenas, cuarentena = revisar([fila, dict(fila)])
    assert len(buenas) == 1
    assert cuarentena == []


def test_id_repetido_con_datos_distintos_todos_a_cuarentena():
    """Mismo ID con datos distintos: ninguna de las filas se acepta."""
    a = fila_valida(tenure="12")
    b = fila_valida(tenure="13")
    buenas, cuarentena = revisar([a, b, fila_valida(customerID="0002-BBBBB")])
    assert [f["customerID"] for f in buenas] == ["0002-BBBBB"]
    assert [r["regla"] for r in cuarentena] == ["id_duplicado_conflictivo"] * 2


def test_cada_fila_queda_en_un_solo_lugar():
    """Buenas + filas con reglas = filas de entrada (tras quitar duplicados)."""
    filas = [
        fila_valida(customerID="0001-AAAAA"),
        fila_valida(customerID="0002-BBBBB", Contract="1 year"),
        fila_valida(customerID="0003-CCCCC", tenure="75", PaymentMethod="PSE"),
        fila_valida(customerID="0001-AAAAA"),  # duplicado identico
    ]
    buenas, cuarentena = revisar(filas)
    ids_malos = {r["customerID"] for r in cuarentena}
    assert len(buenas) == 1
    assert ids_malos == {"0002-BBBBB", "0003-CCCCC"}
    assert len(buenas) + len(ids_malos) == 3
    assert len(cuarentena) == 3  # 1 regla + 2 reglas


def test_registro_de_cuarentena_tiene_las_8_claves():
    """Cada registro de cuarentena trae las 8 claves acordadas."""
    _, cuarentena = revisar([fila_valida(Contract="1 year")])
    r = cuarentena[0]
    assert set(r) == CLAVES_CUARENTENA
    assert r["unidad_etapa"] == "u4_revision"
    assert r["severidad"] == "bloquea"
    assert r["archivo_origen"] == "clientes_2026-09-07.csv"
    assert r["id_ejecucion"] == "run_1"
    assert r["customerID"] == "0001-AAAAA"


def test_fecha_error_es_la_misma_en_todos_los_registros():
    """La fecha del error se calcula una vez por revision."""
    _, cuarentena = revisar([fila_valida(Contract="1 year", tenure="75")])
    assert len(cuarentena) == 2
    assert len({r["fecha_error"] for r in cuarentena}) == 1


def test_registro_original_se_puede_recuperar():
    """registro_original guarda la fila tal cual llego, en JSON."""
    fila = fila_valida(customerID="", Contract="1 year", TotalCharges="")
    _, cuarentena = revisar([fila])
    assert all(json.loads(r["registro_original"]) == fila for r in cuarentena)


def test_customer_id_vacio_es_none_en_cuarentena():
    """Sin ID, el registro de cuarentena guarda customerID como None."""
    _, cuarentena = revisar([fila_valida(customerID="")])
    assert cuarentena[0]["regla"] == "cliente_sin_id"
    assert cuarentena[0]["customerID"] is None


def test_buenas_conservan_columnas_extra_y_orden():
    """Las filas buenas salen iguales, con BancoPago, en el orden de entrada."""
    a = fila_valida(customerID="0001-AAAAA", BancoPago="X")
    b = fila_valida(customerID="0002-BBBBB", BancoPago="Y")
    buenas, cuarentena = revisar([a, b])
    assert buenas == [a, b]
    assert cuarentena == []


# --- Pieza 4: revisar_lote ---

def test_lote_escribe_revisado_y_cuarentena(tmp_path):
    """Un lote con una fila mala genera el CSV revisado y el JSONL."""
    filas = [
        fila_valida(customerID="0001-AAAAA"),
        fila_valida(customerID="0002-BBBBB", Contract="1 year"),
    ]
    ruta = escribir_csv(tmp_path / "clientes_2026-09-07.csv", filas)
    salida = tmp_path / "salida"
    res = revisar_lote(ruta, str(salida), "run_1")
    assert res == {"fecha": FECHA, "filas_leidas": 2, "buenas": 1,
                   "cuarentena": 1, "avisos": 0}
    with open(salida / "revisado/crm/clientes_2026-09-07.csv",
              encoding="utf-8", newline="") as fh:
        leidas = list(csv.DictReader(fh))
    assert [f["customerID"] for f in leidas] == ["0001-AAAAA"]
    registros = leer_jsonl(salida / "cuarentena/u4_revision/clientes_2026-09-07.jsonl")
    assert [r["regla"] for r in registros] == ["categoria_no_permitida_contract"]
    assert registros[0]["archivo_origen"] == ruta


def test_lote_con_columna_extra_avisa_y_la_conserva(tmp_path):
    """BancoPago genera un aviso y la columna pasa al CSV revisado."""
    resto = [c for c in COLUMNAS_CRM if c not in ("customerID", "fecha_lote")]
    enc = ["customerID", "fecha_lote"] + resto + ["BancoPago"]
    ruta = escribir_csv(tmp_path / "clientes_2026-09-07.csv",
                        [fila_valida(BancoPago="Davivienda")], enc)
    salida = tmp_path / "salida"
    res = revisar_lote(ruta, str(salida), "run_1")
    assert res["avisos"] == 1 and res["buenas"] == 1
    with open(salida / "revisado/crm/clientes_2026-09-07.csv",
              encoding="utf-8", newline="") as fh:
        lector = csv.DictReader(fh)
        filas = list(lector)
        assert lector.fieldnames == enc
    assert filas[0]["BancoPago"] == "Davivienda"
    registros = leer_jsonl(salida / "cuarentena/u4_revision/clientes_2026-09-07.jsonl")
    assert [r["regla"] for r in registros] == ["columna_nueva"]
    assert registros[0]["customerID"] is None
    assert json.loads(registros[0]["registro_original"]) == {"encabezados": enc}


def test_repetir_el_lote_reemplaza_la_cuarentena(tmp_path):
    """Correr el lote dos veces no duplica lineas en el JSONL."""
    ruta = escribir_csv(tmp_path / "clientes_2026-09-07.csv",
                        [fila_valida(Contract="1 year")])
    salida = tmp_path / "salida"
    revisar_lote(ruta, str(salida), "run_1")
    revisar_lote(ruta, str(salida), "run_2")
    registros = leer_jsonl(salida / "cuarentena/u4_revision/clientes_2026-09-07.jsonl")
    assert len(registros) == 1
    assert registros[0]["id_ejecucion"] == "run_2"


def test_columna_faltante_bloquea_el_lote(tmp_path):
    """Sin TotalCharges se bloquea: hay JSONL pero no CSV revisado."""
    enc = [c for c in COLUMNAS_CRM if c != "TotalCharges"]
    ruta = escribir_csv(tmp_path / "clientes_2026-09-07.csv", [fila_valida()], enc)
    salida = tmp_path / "salida"
    with pytest.raises(LoteBloqueado, match="TotalCharges"):
        revisar_lote(ruta, str(salida), "run_1")
    registros = leer_jsonl(salida / "cuarentena/u4_revision/clientes_2026-09-07.jsonl")
    assert [r["regla"] for r in registros] == ["columna_faltante"]
    assert not (salida / "revisado/crm/clientes_2026-09-07.csv").exists()


def test_nombre_de_archivo_invalido(tmp_path):
    """El archivo debe llamarse clientes_AAAA-MM-DD.csv."""
    ruta = escribir_csv(tmp_path / "clientes_semana10.csv", [fila_valida()])
    with pytest.raises(ValueError):
        revisar_lote(ruta, str(tmp_path / "salida"), "run_1")
