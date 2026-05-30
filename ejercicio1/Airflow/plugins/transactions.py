"""
Lógica de limpieza y transformación del dataset de transacciones.
"""

import io
import logging

import polars as pl

logger = logging.getLogger(__name__)

# Valores válidos de status (confirmados con el negocio)
VALID_STATUSES = {
    "paid", "voided", "pending_payment", "pre_authorized",
    "refunded", "partially_refunded", "expired", "charged_back",
}

# Corrección de nombres detectados validados con el negocio
NAME_CORRECTIONS = {
    "MiP0xFFFF":   "MiPasajefy",
    "MiPas0xFFFF": "MiPasajefy",
}

REQUIRED_COLUMNS = {
    "id",
    "name",
    "company_id",
    "amount",
    "status",
    "created_at",
    "paid_at",
}

MONEY_COLUMNS = [
    "monto_total",
    "monto_promedio",
    "monto_maximo",
    "monto_minimo",
    "monto_pagado",
    "monto_promedio_pagado",
]

MAX_DECIMAL_18_2 = 9_999_999_999_999_999.99


def read_transactions(raw_bytes: bytes) -> pl.DataFrame:
    """Lee las transacciones desde bytes CSV."""
    df = pl.read_csv(
        io.BytesIO(raw_bytes),
        null_values=[""],
        infer_schema_length=10000,
    )
    logger.info("[transactions] Registros originales: %s", df.shape[0])
    return df


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normaliza encabezados con espacios o caracteres invisibles."""
    normalized_columns = {column: column.strip() for column in df.columns}
    return df.rename(normalized_columns)


def normalize_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Normaliza espacios en campos de texto y convierte blancos en nulos."""
    return df.with_columns(
        pl.when(pl.col(pl.String).str.strip_chars() == "")
        .then(None)
        .otherwise(pl.col(pl.String).str.strip_chars())
        .name.keep()
    )


def drop_empty_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Elimina filas completamente vacías después de normalizar strings."""
    before = df.shape[0]
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))
    logger.info("[transactions] Filas vacías eliminadas: %s", before - df.shape[0])
    return df


def validate_schema(df: pl.DataFrame) -> None:
    """Valida que el CSV contenga las columnas esperadas."""
    columns = set(df.columns)
    missing_columns = sorted(REQUIRED_COLUMNS - columns)
    if missing_columns:
        raise ValueError(
            "El CSV no contiene las columnas requeridas: "
            + ", ".join(missing_columns)
            + ". Columnas recibidas: "
            + ", ".join(df.columns)
        )


def clean_transactions(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica reglas de limpieza al dataset de transacciones.
    """
    # Corregir nombres
    df = df.with_columns(
        pl.col("name").replace(NAME_CORRECTIONS).alias("name")
    )

    # Normalizar company_id
    df = df.with_columns(
        pl.when(
            pl.col("company_id").is_null() |
            (pl.col("company_id") == "*******")
        )
        .then(pl.lit("unknown"))
        .otherwise(pl.col("company_id"))
        .alias("company_id")
    )

    # Limpiar status inválidos
    before = df.shape[0]
    df = df.filter(pl.col("status").is_in(list(VALID_STATUSES)))
    logger.info(
        "[transactions] Filas eliminadas por status inválido: %s",
        before - df.shape[0],
    )

    # Normalizar amount
    df = df.with_columns(
        pl.col("amount").cast(pl.Float64, strict=False).alias("amount")
    )

    invalid_amount = (
        pl.col("amount").is_nan() |
        pl.col("amount").is_infinite() |
        (pl.col("amount").abs() > MAX_DECIMAL_18_2)
    )
    out_of_range_amounts = df.select(invalid_amount.sum()).item()

    df = df.with_columns(
        pl.when(invalid_amount)
        .then(None)
        .otherwise(pl.col("amount"))
        .alias("amount")
    )

    # Cast fechas
    df = df.with_columns([
        pl.col("created_at").str.to_date(format="%Y-%m-%d", strict=False),
        pl.col("paid_at").str.to_date(format="%Y-%m-%d", strict=False),
    ])

    # Metricas de calidad
    null_metrics = df.select(
        pl.col("created_at").is_null().sum().alias("created_at_nulls"),
        pl.col("paid_at").is_null().sum().alias("paid_at_nulls"),
        pl.col("amount").is_null().sum().alias("amount_nulls"),
        (pl.col("amount") < 0).sum().alias("negative_amounts"),
    ).to_dicts()[0]
    logger.info("[transactions] Registros finales: %s", df.shape[0])
    logger.info(
        "[transactions] Montos inválidos o fuera de rango eliminados: %s",
        out_of_range_amounts,
    )
    logger.info("[transactions] Métricas de nulos: %s", null_metrics)

    return df


def aggregate_transactions(df: pl.DataFrame) -> pl.DataFrame:
    """Agrupa métricas transaccionales por name y fecha de creación."""
    before = df.shape[0]
    df = df.filter(
        pl.col("name").is_not_null() &
        pl.col("created_at").is_not_null()
    )
    logger.info(
        "[transactions] Filas excluidas sin llaves de agregación: %s",
        before - df.shape[0],
    )

    agg = (
        df.group_by(["name", "created_at"])
        .agg([
            pl.len().alias("total_transacciones"),
            pl.col("amount").sum().alias("monto_total"),
            pl.col("amount").mean().alias("monto_promedio"),
            pl.col("amount").max().alias("monto_maximo"),
            pl.col("amount").min().alias("monto_minimo"),
            (
                pl.col("status")
                .filter(pl.col("status") == "paid")
                .len()
                .alias("transacciones_pagadas")
            ),
            (
                pl.col("amount")
                .filter(pl.col("status") == "paid")
                .sum()
                .alias("monto_pagado")
            ),
            (
                pl.col("amount")
                .filter(pl.col("status") == "paid")
                .mean()
                .alias("monto_promedio_pagado")
            ),
        ])
        .sort(["name", "created_at"])
    )
    agg = agg.with_columns(
        pl.col(MONEY_COLUMNS).round(2).cast(pl.Decimal(18, 2))
    )
    logger.info("[transactions] Registros agregados: %s", agg.shape[0])
    # logger.info("[transactions] Agregaciones:\n%s", agg)
    return agg


def to_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serializa un DataFrame a Parquet en memoria."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    return buf.read()


def build_transactions_aggregation(raw_bytes: bytes) -> bytes:
    """
    Recibe el CSV como bytes, limpia, agrega por name y created_at,
    y retorna el resultado serializado como Parquet.
    """
    df = read_transactions(raw_bytes)
    df = normalize_columns(df)
    df = normalize_strings(df)
    df = drop_empty_rows(df)
    validate_schema(df)
    clean_df = clean_transactions(df)
    agg_df = aggregate_transactions(clean_df)
    return to_parquet_bytes(agg_df)
