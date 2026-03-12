from pathlib import Path

import pandas as pd

from src.services.texto_service import simplificar_texto
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


COLUNAS_CANONICAS = [
    'STATUS',
    'P1',
    'P2',
    'P3',
    'P4',
    'P5',
    'P6',
]

COLUNAS_SAIDA = [
    'TOTAL',
    'RESPOSTAS',
    'NQA',
    'LIDA_SEM_RESPOSTA',
    'SEM_RETORNO',
    'TOTAL_P1_1',
    'TOTAL_P1_2',
    'TOTAL_P1_3',
    'TOTAL_P1_4',
    'TOTAL_P1_5',
    'TOTAL_P2_1',
    'TOTAL_P2_2',
    'TOTAL_P2_3',
    'TOTAL_P2_4',
    'TOTAL_P2_5',
    'TOTAL_P3_1',
    'TOTAL_P3_2',
    'TOTAL_P3_3',
    'TOTAL_P3_4',
    'TOTAL_P3_5',
    'TOTAL_P4_1',
    'TOTAL_P4_2',
    'TOTAL_P4_3',
    'TOTAL_P4_4',
    'TOTAL_P4_5',
    'TOTAL_P5_1',
    'TOTAL_P5_2',
    'TOTAL_P5_3',
    'TOTAL_P5_4',
    'TOTAL_P5_5',
    'TOTAL_P6_1',
    'TOTAL_P6_2',
    'TOTAL_P6_3',
    'TOTAL_P6_4',
    'TOTAL_P6_5',
]


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


def _serie_preenchida(serie):
    return serie.astype(str).str.strip() != ''


def _serie_normalizada(serie):
    return serie.map(simplificar_texto)


def _contagem_numerica_1_5(serie):
    serie_num = pd.to_numeric(serie.astype(str).str.replace(',', '.', regex=False), errors='coerce')
    return {
        1: int((serie_num == 1).sum()),
        2: int((serie_num == 2).sum()),
        3: int((serie_num == 3).sum()),
        4: int((serie_num == 4).sum()),
        5: int((serie_num == 5).sum()),
    }


def gerar_resumo_internacao_csv(
    arquivo_origem_internacao,
    raiz_analise='src/data/analise_dados/internacao',
):
    df = ler_arquivo_csv(arquivo_origem_internacao)
    mapa_colunas, faltando = _resolver_colunas(df)

    status = _serie_texto(df, mapa_colunas['STATUS'])
    p1 = _serie_texto(df, mapa_colunas['P1'])
    p2 = _serie_texto(df, mapa_colunas['P2'])
    p3 = _serie_texto(df, mapa_colunas['P3'])
    p4 = _serie_texto(df, mapa_colunas['P4'])
    p5 = _serie_texto(df, mapa_colunas['P5'])
    p6 = _serie_texto(df, mapa_colunas['P6'])

    p1_ok = _serie_preenchida(p1)
    p2_ok = _serie_preenchida(p2)
    p3_ok = _serie_preenchida(p3)
    p4_ok = _serie_preenchida(p4)
    p5_ok = _serie_preenchida(p5)
    p6_ok = _serie_preenchida(p6)

    sobra_p2 = (~p1_ok) & p2_ok
    sobra_p3 = (~p1_ok) & (~p2_ok) & p3_ok
    sobra_p4 = (~p1_ok) & (~p2_ok) & (~p3_ok) & p4_ok
    sobra_p5 = (~p1_ok) & (~p2_ok) & (~p3_ok) & (~p4_ok) & p5_ok
    sobra_p6 = (~p1_ok) & (~p2_ok) & (~p3_ok) & (~p4_ok) & (~p5_ok) & p6_ok

    status_norm = _serie_normalizada(status)
    sem_retorno = status.astype(str).str.strip() == ''
    lida_sem_resposta = (status_norm == 'lida') & (~p1_ok)

    p1_cnt = _contagem_numerica_1_5(p1)
    p2_cnt = _contagem_numerica_1_5(p2)
    p3_cnt = _contagem_numerica_1_5(p3)
    p4_cnt = _contagem_numerica_1_5(p4)
    p5_cnt = _contagem_numerica_1_5(p5)
    p6_cnt = _contagem_numerica_1_5(p6)

    linha = {
        'TOTAL': int(len(df)),
        'RESPOSTAS': int(
            p1_ok.sum()
            + sobra_p2.sum()
            + sobra_p3.sum()
            + sobra_p4.sum()
            + sobra_p5.sum()
            + sobra_p6.sum()
        ),
        'NQA': int((status_norm == 'nao_quis').sum()),
        'LIDA_SEM_RESPOSTA': int(lida_sem_resposta.sum()),
        'SEM_RETORNO': int(sem_retorno.sum()),
        'TOTAL_P1_1': p1_cnt[1],
        'TOTAL_P1_2': p1_cnt[2],
        'TOTAL_P1_3': p1_cnt[3],
        'TOTAL_P1_4': p1_cnt[4],
        'TOTAL_P1_5': p1_cnt[5],
        'TOTAL_P2_1': p2_cnt[1],
        'TOTAL_P2_2': p2_cnt[2],
        'TOTAL_P2_3': p2_cnt[3],
        'TOTAL_P2_4': p2_cnt[4],
        'TOTAL_P2_5': p2_cnt[5],
        'TOTAL_P3_1': p3_cnt[1],
        'TOTAL_P3_2': p3_cnt[2],
        'TOTAL_P3_3': p3_cnt[3],
        'TOTAL_P3_4': p3_cnt[4],
        'TOTAL_P3_5': p3_cnt[5],
        'TOTAL_P4_1': p4_cnt[1],
        'TOTAL_P4_2': p4_cnt[2],
        'TOTAL_P4_3': p4_cnt[3],
        'TOTAL_P4_4': p4_cnt[4],
        'TOTAL_P4_5': p4_cnt[5],
        'TOTAL_P5_1': p5_cnt[1],
        'TOTAL_P5_2': p5_cnt[2],
        'TOTAL_P5_3': p5_cnt[3],
        'TOTAL_P5_4': p5_cnt[4],
        'TOTAL_P5_5': p5_cnt[5],
        'TOTAL_P6_1': p6_cnt[1],
        'TOTAL_P6_2': p6_cnt[2],
        'TOTAL_P6_3': p6_cnt[3],
        'TOTAL_P6_4': p6_cnt[4],
        'TOTAL_P6_5': p6_cnt[5],
    }

    pasta_saida = Path(raiz_analise) / 'resumo_internacao'
    arquivo_saida = pasta_saida / 'RESUMO_GERAL_INTERNACAO.csv'
    salvar_dataframe(pd.DataFrame([{k: linha[k] for k in COLUNAS_SAIDA}]), arquivo_saida)

    mensagens = []
    if len(faltando) > 0:
        mensagens.append(
            'Colunas ausentes no arquivo de origem para resumo internacao: '
            + ', '.join(faltando)
        )

    return {
        'ok': True,
        'pasta_saida': str(pasta_saida),
        'arquivos_gerados': [str(arquivo_saida)],
        'mensagens': mensagens,
        'metricas': {
            'resumo_geral_total': linha['TOTAL'],
            'resumo_geral_respostas': linha['RESPOSTAS'],
        },
    }
