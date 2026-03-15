from pathlib import Path

import pandas as pd

from src.services.texto_service import simplificar_texto
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


COLUNAS_CANONICAS = [
    'DT_ENVIO',
    'DT_INTERNACAO',
    'STATUS',
    'P1',
    'P2',
    'P3',
    'P4',
    'TIPO',
    'RP1',
]

COLUNAS_SAIDA_DIA = [
    'TOTAL',
    'RESPOSTAS',
    'NQA',
    'LIDA_SEM_RESPOSTA',
    'SEM_RETORNO',
    'TOTAL_P1_SIM',
    'TOTAL_P1_NAO',
    'TOTAL_P2_SIM',
    'TOTAL_P2_NAO',
    'TOTAL_P3_SIM',
    'TOTAL_P3_NAO',
    'TOTAL_P4_1',
    'TOTAL_P4_2',
    'TOTAL_P4_3',
    'TOTAL_P4_4',
    'TOTAL_P4_5',
    'TOTAL_P4_6',
    'TOTAL_P4_7',
    'TOTAL_P4_8',
    'TOTAL_P4_9',
    'TOTAL_P4_10',
    'MEDIA_P4',
    'TOTAL_VIDEO_SIM',
    'TOTAL_VIDEO_SIM_NQA',
    'TOTAL_VIDEO_SEM_CONTATO',
]

COLUNAS_SAIDA_GERAL = COLUNAS_SAIDA_DIA


def _serie_data(df, coluna):
    if coluna not in df.columns:
        return pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    serie_texto = df[coluna].astype(str).str.strip()
    mask_iso = serie_texto.str.match(r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$')
    serie_data = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    if mask_iso.any():
        serie_data.loc[mask_iso] = pd.to_datetime(
            serie_texto.loc[mask_iso], errors='coerce', dayfirst=False
        )
    if (~mask_iso).any():
        serie_data.loc[~mask_iso] = pd.to_datetime(
            serie_texto.loc[~mask_iso], errors='coerce', dayfirst=True
        )
    return serie_data


def _normalizar_chave_coluna(nome):
    return simplificar_texto(nome).replace(' ', '_')


def _resolver_colunas(df):
    mapa = {_normalizar_chave_coluna(col): col for col in df.columns}
    resolvidas = {}
    faltando = []
    for canonica in COLUNAS_CANONICAS:
        chave = _normalizar_chave_coluna(canonica)
        if chave in mapa:
            resolvidas[canonica] = mapa[chave]
        else:
            resolvidas[canonica] = ''
            faltando.append(canonica)
    return resolvidas, faltando


def _serie_texto(df, coluna_real):
    if not coluna_real:
        return pd.Series('', index=df.index, dtype='object')
    return df[coluna_real].fillna('').astype(str).str.strip()


def _serie_bool_preenchida(serie_texto):
    return serie_texto.astype(str).str.strip() != ''


def _serie_normalizada(serie_texto):
    return serie_texto.map(simplificar_texto)


def _calcular_data_referencia_fixa(dt_envio):
    hoje = pd.Timestamp.now().normalize()
    dt_envio_valido = dt_envio[(dt_envio.notna()) & (dt_envio <= hoje)]
    if len(dt_envio_valido) == 0:
        return pd.NaT, pd.NaT
    max_dt_envio = dt_envio_valido.max()
    data_referencia = max_dt_envio - pd.DateOffset(months=1)
    return max_dt_envio, data_referencia


def _tipos_video_abnominal_aceitos():
    # Mantemos ambos para tolerar variacao historica de nomenclatura.
    return {'video_abnominal', 'video_abdominal'}


def _calcular_metricas_video_resumo_dia(df_base, dt_internacao):
    hoje = pd.Timestamp.now().normalize()
    mes_alvo = (hoje - pd.DateOffset(months=2))
    mask_mes_alvo = (
        dt_internacao.notna()
        & (dt_internacao.dt.year == mes_alvo.year)
        & (dt_internacao.dt.month == mes_alvo.month)
    )

    tipo_norm = _serie_normalizada(_serie_texto(df_base, 'TIPO'))
    p1_norm = _serie_normalizada(_serie_texto(df_base, 'P1'))
    rp1_raw = _serie_texto(df_base, 'RP1')
    rp1_norm = _serie_normalizada(rp1_raw)

    mask_base = (
        mask_mes_alvo
        & tipo_norm.isin(_tipos_video_abnominal_aceitos())
        & (p1_norm == 'sim')
    )

    mask_rp1_vazio = rp1_raw.astype(str).str.strip() == ''
    mask_rp1_nao_quis = rp1_norm == 'nao_quis'
    mask_rp1_outros = (~mask_rp1_vazio) & (~mask_rp1_nao_quis)

    return {
        'TOTAL_VIDEO_SIM': int((mask_base & mask_rp1_outros).sum()),
        'TOTAL_VIDEO_SIM_NQA': int((mask_base & mask_rp1_nao_quis).sum()),
        'TOTAL_VIDEO_SEM_CONTATO': int((mask_base & mask_rp1_vazio).sum()),
    }


def _montar_metricas(df):
    status = _serie_texto(df, 'STATUS')
    p1 = _serie_texto(df, 'P1')
    p2 = _serie_texto(df, 'P2')
    p3 = _serie_texto(df, 'P3')
    p4 = _serie_texto(df, 'P4')
    tipo = _serie_texto(df, 'TIPO')
    rp1 = _serie_texto(df, 'RP1')

    p1_ok = _serie_bool_preenchida(p1)
    p2_ok = _serie_bool_preenchida(p2)
    p3_ok = _serie_bool_preenchida(p3)
    p4_ok = _serie_bool_preenchida(p4)

    sobra_p2 = (~p1_ok) & p2_ok
    sobra_p3 = (~p1_ok) & (~p2_ok) & p3_ok
    sobra_p4 = (~p1_ok) & (~p2_ok) & (~p3_ok) & p4_ok

    status_norm = _serie_normalizada(status)
    p1_norm = _serie_normalizada(p1)
    p2_norm = _serie_normalizada(p2)
    p3_norm = _serie_normalizada(p3)
    tipo_norm = _serie_normalizada(tipo)
    rp1_norm = _serie_normalizada(rp1)

    sem_retorno = (status.astype(str).str.strip() == '')
    status_lida = status_norm == 'lida'

    p4_numerico = pd.to_numeric(p4.astype(str).str.replace(',', '.', regex=False), errors='coerce')
    p4_valido = p4_numerico[(p4_numerico >= 1) & (p4_numerico <= 10)]

    metricas = {
        'TOTAL': int(len(df)),
        'RESPOSTAS': int(p1_ok.sum() + sobra_p2.sum() + sobra_p3.sum() + sobra_p4.sum()),
        'NQA': int((status_norm == 'nao_quis').sum()),
        'LIDA_SEM_RESPOSTA': int((status_lida & (~p1_ok)).sum()),
        'SEM_RETORNO': int(sem_retorno.sum()),
        'TOTAL_P1_SIM': int((p1_norm == 'sim').sum()),
        'TOTAL_P1_NAO': int((p1_norm == 'nao').sum()),
        'TOTAL_P2_SIM': int((p2_norm == 'sim').sum()),
        'TOTAL_P2_NAO': int((p2_norm == 'nao').sum()),
        'TOTAL_P3_SIM': int((p3_norm == 'sim').sum()),
        'TOTAL_P3_NAO': int((p3_norm == 'nao').sum()),
        'TOTAL_P4_1': int((p4_valido == 1).sum()),
        'TOTAL_P4_2': int((p4_valido == 2).sum()),
        'TOTAL_P4_3': int((p4_valido == 3).sum()),
        'TOTAL_P4_4': int((p4_valido == 4).sum()),
        'TOTAL_P4_5': int((p4_valido == 5).sum()),
        'TOTAL_P4_6': int((p4_valido == 6).sum()),
        'TOTAL_P4_7': int((p4_valido == 7).sum()),
        'TOTAL_P4_8': int((p4_valido == 8).sum()),
        'TOTAL_P4_9': int((p4_valido == 9).sum()),
        'TOTAL_P4_10': int((p4_valido == 10).sum()),
        'MEDIA_P4': round(float(p4_valido.mean()), 2) if len(p4_valido) > 0 else 0.0,
        'TOTAL_VIDEO_SIM': int(((p1_norm == 'sim') & (tipo_norm == 'video_abdominal')).sum()),
        'TOTAL_VIDEO_SIM_NQA': int((rp1_norm == 'nao_quis').sum()),
        'TOTAL_VIDEO_SEM_CONTATO': int((rp1.astype(str).str.strip() == '').sum()),
    }
    return metricas


def gerar_resumo_complicacao_csv(
    arquivo_origem_complicacao,
    raiz_analise='src/data/analise_dados/complicacao',
):
    df = ler_arquivo_csv(arquivo_origem_complicacao)
    mapa_colunas, faltando = _resolver_colunas(df)

    df_base = pd.DataFrame(index=df.index)
    for canonica, real in mapa_colunas.items():
        if real:
            df_base[canonica] = df[real]
        else:
            df_base[canonica] = ''

    dt_envio = _serie_data(df_base, 'DT_ENVIO')
    dt_internacao = _serie_data(df_base, 'DT_INTERNACAO')
    max_dt_envio, data_referencia = _calcular_data_referencia_fixa(dt_envio)
    mask_dia = dt_internacao.notna() & pd.notna(data_referencia) & (dt_internacao <= data_referencia)

    metricas_dia = _montar_metricas(df_base.loc[mask_dia].copy())
    metricas_geral = _montar_metricas(df_base.copy())
    metricas_video_dia = _calcular_metricas_video_resumo_dia(df_base, dt_internacao)
    metricas_dia.update(metricas_video_dia)

    pasta_saida = Path(raiz_analise) / 'resumo_complicacao'
    arquivo_dia = pasta_saida / 'RESUMO_DIA_COMPLICACAO.csv'
    arquivo_geral = pasta_saida / 'RESUMO_GERAL_COMPLICACAO.csv'

    salvar_dataframe(pd.DataFrame([{k: metricas_dia[k] for k in COLUNAS_SAIDA_DIA}]), arquivo_dia)
    salvar_dataframe(pd.DataFrame([{k: metricas_geral[k] for k in COLUNAS_SAIDA_GERAL}]), arquivo_geral)

    mensagens = []
    if len(faltando) > 0:
        mensagens.append(
            'Colunas ausentes no arquivo de origem para resumo complicacao: '
            + ', '.join(faltando)
        )

    return {
        'ok': True,
        'pasta_saida': str(pasta_saida),
        'arquivos_gerados': [str(arquivo_dia), str(arquivo_geral)],
        'mensagens': mensagens,
        'metricas': {
            'resumo_dia_total': metricas_dia['TOTAL'],
            'resumo_geral_total': metricas_geral['TOTAL'],
            'max_dt_envio_utilizado': (
                max_dt_envio.strftime('%Y-%m-%d') if pd.notna(max_dt_envio) else ''
            ),
            'data_referencia_utilizada': (
                data_referencia.strftime('%Y-%m-%d') if pd.notna(data_referencia) else ''
            ),
        },
    }
