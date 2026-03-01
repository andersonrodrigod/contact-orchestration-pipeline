import pandas as pd

from src.services.normalizacao_services import normalizar_telefone
from src.services.texto_service import limpar_valor_texto, normalizar_texto_serie, simplificar_texto


STATUS_COLUNAS = {
    'lida': 'LIDA',
    'entregue': 'ENTREGUE',
    'enviada': 'ENVIADA',
    'a meta decidiu nao entregar a mensagem': 'NAO_ENTREGUE_META',
    'mensagem nao pode ser entregue': 'MENSAGEM_NAO_ENTREGUE',
    'numero e parte de um experimento': 'EXPERIMENTO',
    'usuario decidiu nao receber mkt messages': 'OPT_OUT',
}
COLUNAS_STATUS_MAPEADAS = list(dict.fromkeys(STATUS_COLUNAS.values()))

COL_LIDA_RESPOSTA_SIM = 'LIDA_RESPOSTA_SIM'
COL_LIDA_RESPOSTA_NAO = 'LIDA_RESPOSTA_NAO'
COL_LIDA_SEM_RESPOSTA = 'LIDA_SEM_RESPOSTA'


def _normalizar_status_para_mapa(valor_status):
    texto_original = limpar_valor_texto(valor_status)
    if texto_original == '':
        return ''

    chave_status = simplificar_texto(texto_original)
    status_mapeado = STATUS_COLUNAS.get(chave_status)
    if status_mapeado:
        return status_mapeado

    # Ignora status fora do dicionario para nao criar colunas fora do schema final.
    return ''


def _normalizar_resposta_lida(valor_resposta):
    texto = simplificar_texto(valor_resposta)
    if texto in {'sim', 's'}:
        return 'SIM'
    if texto in {'nao', 'n'}:
        return 'NAO'
    if texto in {'sem resposta', 'sem_resposta', ''}:
        return 'SEM_RESPOSTA'
    return ''


def _preencher_contagem_sem_zero(df, coluna, serie_contagem):
    if coluna not in df.columns:
        df[coluna] = ''
    df[coluna] = ''
    serie_positiva = serie_contagem[serie_contagem > 0]
    if len(serie_positiva) > 0:
        df.loc[serie_positiva.index, coluna] = serie_positiva.astype(int)


def _preencher_contagens_status_mapeado(df_destino, df_origem, prefixo=''):
    for coluna_destino in COLUNAS_STATUS_MAPEADAS:
        serie = (
            df_origem[df_origem['__STATUS_MAPEADO'] == coluna_destino]
            .groupby('__ROW_ID')
            .size()
        )
        _preencher_contagem_sem_zero(df_destino, f'{prefixo}{coluna_destino}', serie)


def _preencher_contagens_lida_resposta(df_destino, df_origem, prefixo=''):
    df_lida = df_origem[df_origem['__STATUS_MAPEADO'] == 'LIDA']
    _preencher_contagem_sem_zero(
        df_destino,
        f'{prefixo}{COL_LIDA_RESPOSTA_SIM}',
        df_lida[df_lida['__RESPOSTA_LIDA'] == 'SIM'].groupby('__ROW_ID').size(),
    )
    _preencher_contagem_sem_zero(
        df_destino,
        f'{prefixo}{COL_LIDA_RESPOSTA_NAO}',
        df_lida[df_lida['__RESPOSTA_LIDA'] == 'NAO'].groupby('__ROW_ID').size(),
    )
    _preencher_contagem_sem_zero(
        df_destino,
        f'{prefixo}{COL_LIDA_SEM_RESPOSTA}',
        df_lida[df_lida['__RESPOSTA_LIDA'] == 'SEM_RESPOSTA'].groupby('__ROW_ID').size(),
    )


def _normalizar_status_para_contagens(df_status_full):
    df_status = df_status_full.copy()
    col_resposta = 'Resposta' if 'Resposta' in df_status.columns else 'RESPOSTA'
    if col_resposta not in df_status.columns:
        df_status[col_resposta] = ''

    df_status['Contato'] = normalizar_texto_serie(df_status.get('Contato', pd.Series(dtype=str)))
    df_status['Telefone'] = normalizar_texto_serie(df_status.get('Telefone', pd.Series(dtype=str))).apply(
        normalizar_telefone
    )
    df_status['Status'] = normalizar_texto_serie(df_status.get('Status', pd.Series(dtype=str)))
    df_status[col_resposta] = normalizar_texto_serie(df_status[col_resposta])
    df_status['__STATUS_MAPEADO'] = df_status['Status'].apply(_normalizar_status_para_mapa)
    df_status['__RESPOSTA_LIDA'] = df_status[col_resposta].apply(_normalizar_resposta_lida)
    return df_status


def aplicar_contagens_status(df_saida, df_status_full):
    colunas_obrigatorias_saida = ['CHAVE STATUS', 'TELEFONE ENVIADO']
    faltando_saida = [c for c in colunas_obrigatorias_saida if c not in df_saida.columns]
    if len(faltando_saida) > 0:
        return {
            'ok': False,
            'mensagens': [f'Colunas obrigatorias ausentes no dataset para contagem: {faltando_saida}'],
        }

    colunas_obrigatorias_status = ['Contato', 'Telefone', 'Status']
    faltando_status = [c for c in colunas_obrigatorias_status if c not in df_status_full.columns]
    if len(faltando_status) > 0:
        return {
            'ok': False,
            'mensagens': [f'Colunas obrigatorias ausentes no status para contagem: {faltando_status}'],
        }

    df_status = _normalizar_status_para_contagens(df_status_full)
    colunas_status_join = ['Contato', 'Telefone', '__STATUS_MAPEADO', '__RESPOSTA_LIDA']

    df_base = df_saida.copy()
    df_base['__ROW_ID'] = df_base.index
    df_base['__CHAVE_STATUS_NORM'] = normalizar_texto_serie(df_base['CHAVE STATUS'])
    df_base['__TEL_ENVIADO_NORM'] = normalizar_texto_serie(df_base['TELEFONE ENVIADO']).apply(normalizar_telefone)

    # Join principal: CHAVE STATUS + TELEFONE ENVIADO
    df_join_envio = df_base.merge(
        df_status[colunas_status_join],
        left_on=['__CHAVE_STATUS_NORM', '__TEL_ENVIADO_NORM'],
        right_on=['Contato', 'Telefone'],
        how='left',
    )
    df_join_envio_ok = df_join_envio[
        df_join_envio['__STATUS_MAPEADO'].notna() & (df_join_envio['__STATUS_MAPEADO'] != '')
    ]
    if len(df_join_envio_ok) == 0:
        return {
            'ok': False,
            'mensagens': ['Nenhuma correspondencia encontrada para CHAVE STATUS e TELEFONE ENVIADO no arquivo status.'],
        }

    _preencher_contagens_status_mapeado(df_saida, df_join_envio_ok)
    _preencher_contagens_lida_resposta(df_saida, df_join_envio_ok)

    # Contagem geral por CHAVE STATUS (sem telefone)
    df_join_chave = df_base.merge(
        df_status[colunas_status_join],
        left_on='__CHAVE_STATUS_NORM',
        right_on='Contato',
        how='left',
    )
    df_join_chave_ok = df_join_chave[
        df_join_chave['__STATUS_MAPEADO'].notna() & (df_join_chave['__STATUS_MAPEADO'] != '')
    ]

    _preencher_contagens_status_mapeado(df_saida, df_join_chave_ok, prefixo='QT ')
    _preencher_contagens_lida_resposta(df_saida, df_join_chave_ok, prefixo='QT ')

    qt_telefones = (
        df_status[(df_status['Contato'] != '') & (df_status['Telefone'] != '')]
        .groupby('Contato')['Telefone']
        .nunique()
    )
    _preencher_contagem_sem_zero(
        df_saida,
        'QT TELEFONES',
        df_base.set_index('__ROW_ID')['__CHAVE_STATUS_NORM'].map(qt_telefones).fillna(0),
    )

    return {'ok': True, 'mensagens': []}
