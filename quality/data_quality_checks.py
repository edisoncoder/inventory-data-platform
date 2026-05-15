import pandas as pd
from ingestion.load_supply_chain_csv import TEXT_COLUMNS, NUMERIC_COLUMNS, IDENTIFIER_COLUMNS

def run_quality_checks(df):
    """Executa verificações de qualidade e retorna métricas especificadas."""
    issues = []
    row_count = len(df)
    null_counts = df.isnull().sum().to_dict()
    duplicate_count = df[IDENTIFIER_COLUMNS].duplicated().sum()
    invalid_numeric_count = 0
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            invalid = pd.to_numeric(df[col], errors='coerce').isnull().sum()
            invalid_numeric_count += invalid
    if any(null_counts.values()):
        issues.append('Valores nulos encontrados em algumas colunas.')
    if duplicate_count > 0:
        issues.append(f'{duplicate_count} linhas duplicadas baseadas em {IDENTIFIER_COLUMNS}.')
    if invalid_numeric_count > 0:
        issues.append(f'{invalid_numeric_count} valores inválidos em colunas numéricas.')
    quality_status = 'PASS' if not issues else 'FAIL'
    print(f'Status de qualidade: {quality_status}')
    if issues:
        print('Problemas identificados:', issues)
    return {
        'quality_status': quality_status,
        'issues': issues,
        'row_count': row_count,
        'null_counts': null_counts,
        'duplicate_count': duplicate_count,
        'invalid_numeric_count': invalid_numeric_count
    }