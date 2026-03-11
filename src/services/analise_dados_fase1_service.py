from datetime import datetime
from pathlib import Path

import pandas as pd

from src.services.texto_service import normalizar_texto_serie, simplificar_texto
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe

RESPOSTAS_CANONICAS_PADRAO = ['Sim', 'Nao', 'Sem resposta', 'Nao tenho interesse']
MAPA_RESPOSTAS = {
    'sim': 'Sim',
    'nao': 'Nao',
    'sem resposta': 'Sem resposta',
    'nao tenho interesse': 'Nao tenho interesse',
}


def _sanitizar_nome_processo(nome_processo):
    base = simplificar_texto(nome_processo).replace(' ', '_')
    if base == '':
        return 'execucao'
    return base


def _resolver_nome_execucao(nome_execucao=None, nome_processo='execucao'):
    if nome_execucao:
        return str(nome_execucao).strip()
    prefixo = _sanitizar_nome_processo(nome_processo)
    return f"{prefixo}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"


def _normalizar_dt_envio(df):
    if 'DT ENVIO' not in df.columns:
        return pd.Series('SEM_DATA', index=df.index, dtype='object')
    serie_dt = pd.to_datetime(df['DT ENVIO'].astype(str).str.strip(), errors='coerce', dayfirst=True)
    return serie_dt.dt.strftime('%Y-%m-%d').fillna('SEM_DATA')


def _normalizar_status(df):
    if 'Status' not in df.columns:
        return pd.Series('', index=df.index, dtype='object')
    return normalizar_texto_serie(df['Status'])


def _normalizar_resposta(df):
    if 'RESPOSTA' not in df.columns:
        return pd.Series('Sem resposta', index=df.index, dtype='object')

    serie = normalizar_texto_serie(df['RESPOSTA'])
    chave = serie.apply(simplificar_texto)
    resposta = chave.map(MAPA_RESPOSTAS).fillna(serie)
    resposta = resposta.replace({'': 'Sem resposta'})
    return resposta


def _salvar_tabelas_metricas(
    pasta_saida,
    total_status,
    total_status_resposta,
    com_match,
    sem_match,
    contagem_status,
    contagem_status_por_dt,
    total_lida,
    total_lida_por_dt,
    contagem_resposta_lida,
    contagem_resposta_lida_por_dt,
):
    salvar_dataframe(
        pd.DataFrame(
            [
                {'metrica': 'total_linhas_status', 'valor': int(total_status)},
                {'metrica': 'total_linhas_status_resposta', 'valor': int(total_status_resposta)},
                {'metrica': 'total_registros_com_correspondencia', 'valor': int(com_match)},
                {'metrica': 'total_registros_sem_correspondencia', 'valor': int(sem_match)},
                {'metrica': 'total_status_lida', 'valor': int(total_lida)},
            ]
        ),
        pasta_saida / 'fase1_resumo_totais.csv',
    )
    salvar_dataframe(contagem_status, pasta_saida / 'fase1_contagem_status.csv')
    salvar_dataframe(contagem_status_por_dt, pasta_saida / 'fase1_contagem_status_por_dt_envio.csv')
    salvar_dataframe(
        total_lida_por_dt.rename(columns={'total': 'total_status_lida'}),
        pasta_saida / 'fase1_total_status_lida_por_dt_envio.csv',
    )
    salvar_dataframe(contagem_resposta_lida, pasta_saida / 'fase1_contagem_resposta_lida.csv')
    salvar_dataframe(
        contagem_resposta_lida_por_dt,
        pasta_saida / 'fase1_contagem_resposta_lida_por_dt_envio.csv',
    )


def gerar_analise_dados_fase1_csv(
    arquivo_status,
    arquivo_status_resposta,
    arquivo_status_integrado,
    com_match,
    sem_match,
    raiz_analise='src/data/analise_dados',
    nome_execucao=None,
    nome_processo='unificar_status_e_status_resposta',
    respostas_canonicas=None,
):
    nome_execucao = _resolver_nome_execucao(
        nome_execucao=nome_execucao,
        nome_processo=nome_processo,
    )
    if respostas_canonicas is None:
        respostas_canonicas = list(RESPOSTAS_CANONICAS_PADRAO)

    pasta_saida = Path(raiz_analise) / nome_execucao
    pasta_saida.mkdir(parents=True, exist_ok=True)

    df_status = ler_arquivo_csv(arquivo_status)
    df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)
    df_integrado = ler_arquivo_csv(arquivo_status_integrado)

    dt_envio = _normalizar_dt_envio(df_integrado)
    status = _normalizar_status(df_integrado)
    resposta = _normalizar_resposta(df_integrado)

    contagem_status = (
        status.value_counts(dropna=False)
        .rename_axis('status')
        .reset_index(name='total')
        .sort_values(['total', 'status'], ascending=[False, True])
    )

    contagem_status_por_dt = (
        pd.DataFrame({'dt_envio': dt_envio, 'status': status})
        .groupby(['dt_envio', 'status'], dropna=False)
        .size()
        .reset_index(name='total')
        .sort_values(['dt_envio', 'total', 'status'], ascending=[True, False, True])
    )

    mask_lida = status.apply(simplificar_texto) == 'lida'
    total_lida = int(mask_lida.sum())
    total_lida_por_dt = (
        pd.DataFrame({'dt_envio': dt_envio[mask_lida]})
        .groupby(['dt_envio'], dropna=False)
        .size()
        .reset_index(name='total')
        .sort_values(['dt_envio'])
    )

    resposta_lida = resposta[mask_lida]
    resposta_lida_filtrada = resposta_lida[resposta_lida.isin(set(respostas_canonicas))]
    contagem_resposta_lida = (
        resposta_lida_filtrada.value_counts(dropna=False)
        .rename_axis('resposta')
        .reset_index(name='total')
    )
    for resposta_canonica in respostas_canonicas:
        if resposta_canonica not in set(contagem_resposta_lida['resposta']):
            contagem_resposta_lida = pd.concat(
                [
                    contagem_resposta_lida,
                    pd.DataFrame([{'resposta': resposta_canonica, 'total': 0}]),
                ],
                ignore_index=True,
            )
    contagem_resposta_lida = contagem_resposta_lida.sort_values(
        ['total', 'resposta'], ascending=[False, True]
    ).reset_index(drop=True)

    contagem_resposta_lida_por_dt = (
        pd.DataFrame({'dt_envio': dt_envio[mask_lida], 'resposta': resposta_lida})
        .query('resposta in @respostas_canonicas')
        .groupby(['dt_envio', 'resposta'], dropna=False)
        .size()
        .reset_index(name='total')
        .sort_values(['dt_envio', 'total', 'resposta'], ascending=[True, False, True])
    )

    _salvar_tabelas_metricas(
        pasta_saida=pasta_saida,
        total_status=len(df_status),
        total_status_resposta=len(df_status_resposta),
        com_match=int(com_match),
        sem_match=int(sem_match),
        contagem_status=contagem_status,
        contagem_status_por_dt=contagem_status_por_dt,
        total_lida=total_lida,
        total_lida_por_dt=total_lida_por_dt,
        contagem_resposta_lida=contagem_resposta_lida,
        contagem_resposta_lida_por_dt=contagem_resposta_lida_por_dt,
    )

    return {
        'ok': True,
        'nome_execucao': nome_execucao,
        'pasta_saida': str(pasta_saida),
        'arquivos_gerados': [
            str(pasta_saida / 'fase1_resumo_totais.csv'),
            str(pasta_saida / 'fase1_contagem_status.csv'),
            str(pasta_saida / 'fase1_contagem_status_por_dt_envio.csv'),
            str(pasta_saida / 'fase1_total_status_lida_por_dt_envio.csv'),
            str(pasta_saida / 'fase1_contagem_resposta_lida.csv'),
            str(pasta_saida / 'fase1_contagem_resposta_lida_por_dt_envio.csv'),
        ],
    }
