from pathlib import Path

import pandas as pd

from core.error_codes import (
    ERRO_INGESTAO,
    ERRO_QUALIDADE_DATA,
    ERRO_VALIDACAO_ARQUIVOS,
    ERRO_VALIDACAO_COLUNAS,
)
from core.logger import PipelineLogger
from src.config.governanca_config import resolver_limiar_nat_data
from src.config.paths import DEFAULTS_COMPLICACAO, DEFAULTS_INTERNACAO_ELETIVO
from src.contracts.preflight_contracts import build_preflight_result
from src.services.validacao_service import (
    validar_colunas_origem_dataset_complicacao,
    validar_colunas_origem_para_padronizacao,
)
from src.services.schema_resposta_service import COLUNA_DATA_ATENDIMENTO_CANONICA
from src.utils.arquivos import ler_arquivo_csv, validar_arquivos_existem


def _coluna_data_resposta(df_status_resposta):
    if COLUNA_DATA_ATENDIMENTO_CANONICA in df_status_resposta.columns:
        return COLUNA_DATA_ATENDIMENTO_CANONICA
    if 'dat_atendimento' in df_status_resposta.columns:
        return 'dat_atendimento'
    return None


def _avaliar_percentual_nat(df, coluna):
    if coluna not in df.columns or len(df) == 0:
        return {
            'total': len(df),
            'nat': 0,
            'pct_nat': 0.0,
        }
    serie = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)
    nat = int(serie.isna().sum())
    total = len(df)
    pct_nat = (nat / total) * 100 if total > 0 else 0.0
    return {'total': total, 'nat': nat, 'pct_nat': round(pct_nat, 2)}


def _executar_preflight_com_dataframes(
    contexto,
    df_status,
    df_status_resposta,
    df_origem,
    limiar_nat_data,
    logger,
):
    bloqueios = []
    avisos = []
    detalhes = {}

    validacao_origem = validar_colunas_origem_para_padronizacao(df_status, df_status_resposta)
    if not validacao_origem['ok']:
        bloqueios.extend(validacao_origem['mensagens'])

    validacao_dataset = validar_colunas_origem_dataset_complicacao(df_origem.columns, contexto=contexto)
    if not validacao_dataset['ok']:
        bloqueios.extend(validacao_dataset['mensagens'])

    col_data_status = 'Data agendamento' if 'Data agendamento' in df_status.columns else None
    col_data_resposta = _coluna_data_resposta(df_status_resposta)

    qualidade = {}
    if col_data_status:
        qualidade[col_data_status] = _avaliar_percentual_nat(df_status, col_data_status)
        if qualidade[col_data_status]['pct_nat'] >= limiar_nat_data:
            bloqueios.append(
                (
                    f'Qualidade de data abaixo do esperado em {col_data_status}: '
                    f"{qualidade[col_data_status]['pct_nat']}% NaT (limiar={limiar_nat_data}%)."
                )
            )
    else:
        avisos.append('Coluna Data agendamento ausente em status para avaliacao de qualidade.')

    if col_data_resposta:
        qualidade[col_data_resposta] = _avaliar_percentual_nat(df_status_resposta, col_data_resposta)
        if qualidade[col_data_resposta]['pct_nat'] >= limiar_nat_data:
            bloqueios.append(
                (
                    f'Qualidade de data abaixo do esperado em {col_data_resposta}: '
                    f"{qualidade[col_data_resposta]['pct_nat']}% NaT (limiar={limiar_nat_data}%)."
                )
            )
    else:
        bloqueios.append(
            'Qualidade de data indisponivel: coluna de atendimento ausente em status_resposta.'
        )

    detalhes['qualidade_data'] = qualidade
    detalhes['validacao_colunas_origem'] = validacao_origem
    detalhes['validacao_colunas_dataset'] = validacao_dataset
    detalhes['metricas_por_etapa'] = {
        'preflight': {
            'linhas_status': len(df_status),
            'linhas_status_resposta': len(df_status_resposta),
            'linhas_dataset_origem': len(df_origem),
            'limiar_nat_data_em_uso': limiar_nat_data,
        }
    }

    metricas = {
        'linhas_status': len(df_status),
        'linhas_status_resposta': len(df_status_resposta),
        'linhas_dataset_origem': len(df_origem),
        'limiar_nat_data_em_uso': limiar_nat_data,
    }
    for coluna, dados in qualidade.items():
        chave = coluna.replace(' ', '_').lower()
        metricas[f'pct_nat_{chave}'] = dados['pct_nat']

    ok = len(bloqueios) == 0
    codigo_erro = None
    if not ok:
        if any('qualidade de data' in str(b).lower() for b in bloqueios):
            codigo_erro = ERRO_QUALIDADE_DATA
        elif not validacao_origem['ok'] or not validacao_dataset['ok']:
            codigo_erro = ERRO_VALIDACAO_COLUNAS
    resultado = build_preflight_result(
        ok=ok,
        contexto=contexto,
        bloqueios=bloqueios,
        avisos=avisos,
        metricas=metricas,
        detalhes=detalhes,
        codigo_erro=codigo_erro,
    )
    logger.finalizar('SUCESSO' if ok else 'FALHA_VALIDACAO')
    return resultado


def _arquivo_unificado_esta_atualizado(arquivo_unificado, arquivo_eletivo, arquivo_internacao):
    if not Path(arquivo_unificado).exists():
        return False

    mtime_unificado = Path(arquivo_unificado).stat().st_mtime
    mtime_eletivo = Path(arquivo_eletivo).stat().st_mtime if Path(arquivo_eletivo).exists() else 0
    mtime_internacao = (
        Path(arquivo_internacao).stat().st_mtime if Path(arquivo_internacao).exists() else 0
    )
    return mtime_unificado >= max(mtime_eletivo, mtime_internacao)


def run_preflight_pipeline(
    contexto,
    arquivo_status,
    arquivo_status_resposta,
    arquivo_dataset_origem,
    limiar_nat_data=None,
    nome_logger='preflight_pipeline',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    limiar_nat_data, origem_limiar = resolver_limiar_nat_data(
        limiar_nat_data,
        contexto=contexto,
    )
    logger.info('INICIO', f'contexto={contexto}')
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'arquivo_dataset_origem={arquivo_dataset_origem}')
    logger.info('INICIO', f'limiar_nat_data={limiar_nat_data}')
    logger.info('INICIO', f'limiar_nat_data_origem={origem_limiar}')

    validacao_arquivos = validar_arquivos_existem(
        {
            'arquivo_status': arquivo_status,
            'arquivo_status_resposta': arquivo_status_resposta,
            'arquivo_dataset_origem': arquivo_dataset_origem,
        }
    )
    if not validacao_arquivos['ok']:
        resultado = build_preflight_result(
            ok=False,
            contexto=contexto,
            bloqueios=validacao_arquivos['faltando'],
            avisos=[],
            detalhes={'validacao_arquivos': validacao_arquivos},
            codigo_erro=ERRO_VALIDACAO_ARQUIVOS,
        )
        logger.finalizar('FALHA_ARQUIVOS')
        return resultado

    try:
        df_status = ler_arquivo_csv(arquivo_status)
        df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)
        df_origem = ler_arquivo_csv(arquivo_dataset_origem)
        return _executar_preflight_com_dataframes(
            contexto=contexto,
            df_status=df_status,
            df_status_resposta=df_status_resposta,
            df_origem=df_origem,
            limiar_nat_data=limiar_nat_data,
            logger=logger,
        )
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return build_preflight_result(
            ok=False,
            contexto=contexto,
            bloqueios=[f'Erro inesperado no preflight: {type(erro).__name__}: {erro}'],
            avisos=[],
            detalhes={},
            codigo_erro=ERRO_INGESTAO,
        )


def run_preflight_complicacao(limiar_nat_data=None):
    return run_preflight_pipeline(
        contexto='complicacao',
        arquivo_status=DEFAULTS_COMPLICACAO['arquivo_status'],
        arquivo_status_resposta=DEFAULTS_COMPLICACAO['arquivo_status_resposta_complicacao'],
        arquivo_dataset_origem=DEFAULTS_COMPLICACAO['arquivo_dataset_origem_complicacao'],
        limiar_nat_data=limiar_nat_data,
        nome_logger='preflight_complicacao',
    )


def run_preflight_internacao_eletivo(limiar_nat_data=None):
    arquivo_status = DEFAULTS_INTERNACAO_ELETIVO['arquivo_status']
    arquivo_unificado = DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_unificado']
    arquivo_eletivo = DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_eletivo']
    arquivo_internacao = DEFAULTS_INTERNACAO_ELETIVO['arquivo_status_resposta_internacao']
    arquivo_dataset_origem = DEFAULTS_INTERNACAO_ELETIVO['arquivo_dataset_origem_internacao']

    if _arquivo_unificado_esta_atualizado(arquivo_unificado, arquivo_eletivo, arquivo_internacao):
        return run_preflight_pipeline(
            contexto='internacao_eletivo',
            arquivo_status=arquivo_status,
            arquivo_status_resposta=arquivo_unificado,
            arquivo_dataset_origem=arquivo_dataset_origem,
            limiar_nat_data=limiar_nat_data,
            nome_logger='preflight_internacao_eletivo',
        )

    limiar_nat_data, origem_limiar = resolver_limiar_nat_data(
        limiar_nat_data,
        contexto='internacao_eletivo',
    )
    logger = PipelineLogger(nome_pipeline='preflight_internacao_eletivo')
    if Path(arquivo_unificado).exists():
        logger.warning(
            'INICIO',
            (
                'Arquivo unificado desatualizado em relacao a eletivo/internacao; '
                'usando fallback eletivo+internacao em memoria.'
            ),
        )
    else:
        logger.info('INICIO', 'Arquivo unificado ausente; usando fallback eletivo+internacao em memoria.')
    logger.info('INICIO', f'limiar_nat_data={limiar_nat_data}')
    logger.info('INICIO', f'limiar_nat_data_origem={origem_limiar}')
    validacao_arquivos = validar_arquivos_existem(
        {
            'arquivo_status': arquivo_status,
            'arquivo_status_resposta_eletivo': arquivo_eletivo,
            'arquivo_status_resposta_internacao': arquivo_internacao,
            'arquivo_dataset_origem': arquivo_dataset_origem,
        }
    )
    if not validacao_arquivos['ok']:
        resultado = build_preflight_result(
            ok=False,
            contexto='internacao_eletivo',
            bloqueios=validacao_arquivos['faltando'],
            avisos=[],
            detalhes={'validacao_arquivos': validacao_arquivos},
            codigo_erro=ERRO_VALIDACAO_ARQUIVOS,
        )
        logger.finalizar('FALHA_ARQUIVOS')
        return resultado

    try:
        df_status = ler_arquivo_csv(arquivo_status)
        df_eletivo = ler_arquivo_csv(arquivo_eletivo)
        df_internacao = ler_arquivo_csv(arquivo_internacao)
        colunas_unificadas = sorted(set(df_eletivo.columns).union(set(df_internacao.columns)))
        df_status_resposta = pd.concat(
            [
                df_eletivo.reindex(columns=colunas_unificadas, fill_value=''),
                df_internacao.reindex(columns=colunas_unificadas, fill_value=''),
            ],
            ignore_index=True,
        )
        df_origem = ler_arquivo_csv(arquivo_dataset_origem)
        return _executar_preflight_com_dataframes(
            contexto='internacao_eletivo',
            df_status=df_status,
            df_status_resposta=df_status_resposta,
            df_origem=df_origem,
            limiar_nat_data=limiar_nat_data,
            logger=logger,
        )
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return build_preflight_result(
            ok=False,
            contexto='internacao_eletivo',
            bloqueios=[f'Erro inesperado no preflight: {type(erro).__name__}: {erro}'],
            avisos=[],
            detalhes={},
            codigo_erro=ERRO_INGESTAO,
        )
