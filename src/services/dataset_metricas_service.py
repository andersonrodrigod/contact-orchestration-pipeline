import pandas as pd

from src.services.normalizacao_services import corrigir_texto_bugado, normalizar_telefone


STATUS_COLUNAS = {
    'Lida': 'LIDA',
    'Entregue': 'ENTREGUE',
    'Enviada': 'ENVIADA',
    'A Meta decidiu nao entregar a mensagem': 'NAO_ENTREGUE_META',
    'Mensagem nao pode ser entregue': 'MENSAGEM_NAO_ENTREGUE',
    'Numero e parte de um experimento': 'EXPERIMENTO',
    'Usuario decidiu nao receber MKT messages': 'OPT_OUT',
}


def _normalizar_texto_serie(serie):
    return serie.fillna('').astype(str).str.strip()


def _limpar_valor_texto(valor):
    if pd.isna(valor):
        return ''
    texto = str(valor).strip()
    texto = corrigir_texto_bugado(texto)
    if texto in {'0', '0.0', 'nan', 'NaN', 'None'}:
        return ''
    return texto


def _normalizar_texto_chave(valor):
    texto = _limpar_valor_texto(valor)
    texto = texto.replace('Ã§', 'ç').replace('Ã£', 'ã').replace('Ã¡', 'á').replace('Ã©', 'é').replace('Ãº', 'ú')
    return texto


def _normalizar_status_para_mapa(valor_status):
    texto = _normalizar_texto_chave(valor_status)
    texto = texto.replace('Não', 'Nao').replace('não', 'nao')
    texto = texto.replace('Número', 'Numero').replace('Usuário', 'Usuario').replace('é', 'e')
    return STATUS_COLUNAS.get(texto, '')


def _normalizar_resposta_lida(valor_resposta):
    texto = _normalizar_texto_chave(valor_resposta).lower()
    texto = texto.replace('ã', 'a').replace('á', 'a').replace('â', 'a').replace('é', 'e').replace('í', 'i')
    texto = texto.replace('ó', 'o').replace('ô', 'o').replace('ú', 'u').replace('ç', 'c')
    if texto in {'sim', 's'}:
        return 'SIM'
    if texto in {'nao', 'não', 'n'}:
        return 'NAO'
    if texto in {'sem resposta', 'sem_resposta', ''}:
        return 'SEM_RESPOSTA'
    return ''


def _coluna_existente(df, preferida, alternativa=None):
    if preferida in df.columns:
        return preferida
    if alternativa and alternativa in df.columns:
        return alternativa
    return preferida


def _preencher_contagem_sem_zero(df, coluna, serie_contagem):
    if coluna not in df.columns:
        df[coluna] = ''
    df[coluna] = ''
    serie_positiva = serie_contagem[serie_contagem > 0]
    if len(serie_positiva) > 0:
        df.loc[serie_positiva.index, coluna] = serie_positiva.astype(int)


def aplicar_contagens_status(df_saida, df_status_full):
    df_status = df_status_full.copy()
    col_resposta_status = _coluna_existente(df_status, 'Resposta', 'RESPOSTA')
    if col_resposta_status not in df_status.columns:
        df_status[col_resposta_status] = ''

    df_status['Contato'] = _normalizar_texto_serie(df_status.get('Contato', pd.Series(dtype=str)))
    df_status['Telefone'] = _normalizar_texto_serie(df_status.get('Telefone', pd.Series(dtype=str))).apply(
        normalizar_telefone
    )
    df_status['Status'] = _normalizar_texto_serie(df_status.get('Status', pd.Series(dtype=str))).apply(
        _normalizar_texto_chave
    )
    df_status[col_resposta_status] = _normalizar_texto_serie(df_status[col_resposta_status]).apply(
        _normalizar_texto_chave
    )
    df_status['__STATUS_MAPEADO'] = df_status['Status'].apply(_normalizar_status_para_mapa)
    df_status['__RESPOSTA_LIDA'] = df_status[col_resposta_status].apply(_normalizar_resposta_lida)

    df_base = df_saida.copy()
    df_base['__ROW_ID'] = df_base.index
    df_base['__CHAVE_STATUS_NORM'] = _normalizar_texto_serie(df_base['CHAVE STATUS'])
    df_base['__TEL_ENVIADO_NORM'] = _normalizar_texto_serie(df_base['TELEFONE ENVIADO']).apply(normalizar_telefone)

    # Join principal: CHAVE STATUS + TELEFONE ENVIADO
    df_join_envio = df_base.merge(
        df_status[['Contato', 'Telefone', '__STATUS_MAPEADO', '__RESPOSTA_LIDA']],
        left_on=['__CHAVE_STATUS_NORM', '__TEL_ENVIADO_NORM'],
        right_on=['Contato', 'Telefone'],
        how='left',
    )
    df_join_envio_ok = df_join_envio[df_join_envio['__STATUS_MAPEADO'].notna() & (df_join_envio['__STATUS_MAPEADO'] != '')]
    if len(df_join_envio_ok) == 0:
        return {
            'ok': False,
            'mensagens': ['Nenhuma correspondência encontrada para CHAVE STATUS e TELEFONE ENVIADO no arquivo status.'],
        }

    for coluna_destino in STATUS_COLUNAS.values():
        serie = (
            df_join_envio_ok[df_join_envio_ok['__STATUS_MAPEADO'] == coluna_destino]
            .groupby('__ROW_ID')
            .size()
        )
        _preencher_contagem_sem_zero(df_saida, coluna_destino, serie)

    col_lida_sim = _coluna_existente(df_saida, 'LIDA_RESPOSTA_SIM', 'LIDA_REPOSTA_SIM')
    col_lida_nao = _coluna_existente(df_saida, 'LIDA_RESPOSTA_NAO', 'LIDA_REPOSTA_NAO')
    col_lida_sem = _coluna_existente(df_saida, 'LIDA_SEM_RESPOSTA')
    df_lida = df_join_envio_ok[df_join_envio_ok['__STATUS_MAPEADO'] == 'LIDA']
    _preencher_contagem_sem_zero(
        df_saida,
        col_lida_sim,
        df_lida[df_lida['__RESPOSTA_LIDA'] == 'SIM'].groupby('__ROW_ID').size(),
    )
    _preencher_contagem_sem_zero(
        df_saida,
        col_lida_nao,
        df_lida[df_lida['__RESPOSTA_LIDA'] == 'NAO'].groupby('__ROW_ID').size(),
    )
    _preencher_contagem_sem_zero(
        df_saida,
        col_lida_sem,
        df_lida[df_lida['__RESPOSTA_LIDA'] == 'SEM_RESPOSTA'].groupby('__ROW_ID').size(),
    )

    # Contagem geral por CHAVE STATUS (sem telefone)
    df_join_chave = df_base.merge(
        df_status[['Contato', 'Telefone', '__STATUS_MAPEADO', '__RESPOSTA_LIDA']],
        left_on='__CHAVE_STATUS_NORM',
        right_on='Contato',
        how='left',
    )
    df_join_chave_ok = df_join_chave[df_join_chave['__STATUS_MAPEADO'].notna() & (df_join_chave['__STATUS_MAPEADO'] != '')]

    for coluna_destino in STATUS_COLUNAS.values():
        serie_qt = (
            df_join_chave_ok[df_join_chave_ok['__STATUS_MAPEADO'] == coluna_destino]
            .groupby('__ROW_ID')
            .size()
        )
        _preencher_contagem_sem_zero(df_saida, f'QT {coluna_destino}', serie_qt)

    col_qt_lida_sim = _coluna_existente(df_saida, 'QT LIDA_RESPOSTA_SIM', 'QT LIDA_REPOSTA_SIM')
    col_qt_lida_nao = _coluna_existente(df_saida, 'QT LIDA_RESPOSTA_NAO', 'QT LIDA_REPOSTA_NAO')
    col_qt_lida_sem = _coluna_existente(df_saida, 'QT LIDA_SEM_RESPOSTA')
    df_lida_qt = df_join_chave_ok[df_join_chave_ok['__STATUS_MAPEADO'] == 'LIDA']
    _preencher_contagem_sem_zero(
        df_saida,
        col_qt_lida_sim,
        df_lida_qt[df_lida_qt['__RESPOSTA_LIDA'] == 'SIM'].groupby('__ROW_ID').size(),
    )
    _preencher_contagem_sem_zero(
        df_saida,
        col_qt_lida_nao,
        df_lida_qt[df_lida_qt['__RESPOSTA_LIDA'] == 'NAO'].groupby('__ROW_ID').size(),
    )
    _preencher_contagem_sem_zero(
        df_saida,
        col_qt_lida_sem,
        df_lida_qt[df_lida_qt['__RESPOSTA_LIDA'] == 'SEM_RESPOSTA'].groupby('__ROW_ID').size(),
    )

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
