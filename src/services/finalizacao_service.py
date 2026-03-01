import pandas as pd

from src.services.texto_service import normalizar_texto_serie

VALOR_SEM_TELEFONE_DISPONIVEL = 'SEM_TELEFONE_DISPONIVEL'
MARCADOR_ACAO_PROXIMO = '__PROXIMO__'
MARCADOR_ACAO_PRIORIDADE = '__PRIORIDADE__'
PROCESSO_SEM_TELEFONE = 'SEM_TELEFONE_PARA_MAIS_DISPARO'
PROCESSO_RESOLVIDO = 'RESOLVIDO'
PROCESSOS_PRIORITARIOS = {'ENCERRAR_CONTATO_LIDO_SIM', 'ENCERRAR_CONTATO_NAO_QUIS'}

ABAS_PADRAO = [
    'usuarios',
    'usuarios_lidos',
    'usuarios_respondidos',
    'usuarios_duplicados',
    'usuarios_resolvidos',
]


def _coluna_existente(df, preferida, alternativa=None):
    if preferida in df.columns:
        return preferida
    if alternativa and alternativa in df.columns:
        return alternativa
    return preferida


def _serie_numerica(df, coluna):
    if coluna not in df.columns:
        return pd.Series(0, index=df.index, dtype='float64')
    return pd.to_numeric(df[coluna], errors='coerce').fillna(0)


def _normalizar_resposta_para_regra(serie):
    texto = normalizar_texto_serie(serie).str.lower()
    texto = texto.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii')
    return texto


def _inicializar_colunas_classificacao(df):
    if 'PROCESSO' not in df.columns:
        df['PROCESSO'] = ''
    if 'ACAO' not in df.columns:
        df['ACAO'] = ''
    if 'PROXIMO TELEFONE DISPONIVEL' not in df.columns:
        df['PROXIMO TELEFONE DISPONIVEL'] = VALOR_SEM_TELEFONE_DISPONIVEL
    if 'TELEFONE PRIORIDADE' not in df.columns:
        df['TELEFONE PRIORIDADE'] = ''

    df['PROCESSO'] = normalizar_texto_serie(df['PROCESSO'])
    df['ACAO'] = normalizar_texto_serie(df['ACAO'])


def _obter_serie_somatorio(df):
    s_lida = _serie_numerica(df, 'LIDA')
    s_entregue = _serie_numerica(df, 'ENTREGUE')
    s_enviada = _serie_numerica(df, 'ENVIADA')
    s_nao_entregue_meta = _serie_numerica(df, 'NAO_ENTREGUE_META')
    s_msg_nao_entregue = _serie_numerica(df, 'MENSAGEM_NAO_ENTREGUE')
    s_experimento = _serie_numerica(df, 'EXPERIMENTO')
    s_opt_out = _serie_numerica(df, 'OPT_OUT')
    return (
        s_lida
        + s_entregue
        + s_enviada
        + s_nao_entregue_meta
        + s_msg_nao_entregue
        + s_experimento
        + s_opt_out
    )


def aplicar_classificacao_processo_acao(df):
    df = df.copy()
    _inicializar_colunas_classificacao(df)

    col_lida_sim = _coluna_existente(df, 'LIDA_RESPOSTA_SIM', 'LIDA_REPOSTA_SIM')
    col_lida_nao = _coluna_existente(df, 'LIDA_RESPOSTA_NAO', 'LIDA_REPOSTA_NAO')

    s_lida_sim = _serie_numerica(df, col_lida_sim)
    s_lida_nao = _serie_numerica(df, col_lida_nao)
    s_lida_sem = _serie_numerica(df, 'LIDA_SEM_RESPOSTA')
    s_entregue = _serie_numerica(df, 'ENTREGUE')
    s_enviada = _serie_numerica(df, 'ENVIADA')
    s_nao_entregue_meta = _serie_numerica(df, 'NAO_ENTREGUE_META')
    s_msg_nao_entregue = _serie_numerica(df, 'MENSAGEM_NAO_ENTREGUE')
    s_experimento = _serie_numerica(df, 'EXPERIMENTO')
    s_opt_out = _serie_numerica(df, 'OPT_OUT')
    somatorio_de_valores = _obter_serie_somatorio(df)

    resposta_norm = _normalizar_resposta_para_regra(df.get('RESPOSTA', pd.Series('', index=df.index)))
    proximo_tel = normalizar_texto_serie(df['PROXIMO TELEFONE DISPONIVEL'])
    telefone_prioridade = normalizar_texto_serie(df['TELEFONE PRIORIDADE'])

    regras = [
        (s_lida_sim >= 1, 'ENCERRAR_CONTATO_LIDO_SIM', 'ENCERRADO'),
        (s_lida_nao >= 1, 'MUDAR_CONTATO_LIDO_NAO', MARCADOR_ACAO_PROXIMO),
        (s_lida_sem >= 2, 'MUDAR_CONTATO_LIDO_SEM_RESPOSTA', MARCADOR_ACAO_PROXIMO),
        (s_entregue >= 3, 'MUDAR_CONTATO_ENTREGRUE', MARCADOR_ACAO_PROXIMO),
        (s_enviada >= 3, 'MUDAR_CONTATO_ENVIADO', MARCADOR_ACAO_PROXIMO),
        (s_nao_entregue_meta >= 3, 'MUDAR_CONTATO_NAO_ENTREGUE_META', MARCADOR_ACAO_PROXIMO),
        (s_msg_nao_entregue >= 3, 'MUDAR_CONTATO_MENSAGEM_NAO_ENTREGUE', MARCADOR_ACAO_PROXIMO),
        (s_experimento >= 3, 'MUDAR_CONTATO_EXPERIMENTO', MARCADOR_ACAO_PROXIMO),
        (s_opt_out >= 3, 'MUDAR_CONTATO_OPT_OUT', MARCADOR_ACAO_PROXIMO),
        (resposta_norm == 'nao tenho interesse', 'ENCERRAR_CONTATO_NAO_QUIS', 'ENCERRADO'),
        (somatorio_de_valores >= 4, 'MUDAR_CONTATO_MENSAGEM_NAO_ENTREGUE', MARCADOR_ACAO_PROXIMO),
        (somatorio_de_valores <= 3, 'DISPARAR_NOVAMENTE', MARCADOR_ACAO_PRIORIDADE),
    ]

    for condicao, processo, acao in regras:
        mask = (df['PROCESSO'] == '') & condicao
        if not mask.any():
            continue
        df.loc[mask, 'PROCESSO'] = processo
        if acao == MARCADOR_ACAO_PROXIMO:
            df.loc[mask, 'ACAO'] = proximo_tel[mask]
        elif acao == MARCADOR_ACAO_PRIORIDADE:
            df.loc[mask, 'ACAO'] = telefone_prioridade[mask]
        else:
            df.loc[mask, 'ACAO'] = acao

    mask_sem_telefone = (
        (proximo_tel == VALOR_SEM_TELEFONE_DISPONIVEL)
        & (~df['PROCESSO'].isin(PROCESSOS_PRIORITARIOS))
    )
    df.loc[mask_sem_telefone, 'PROCESSO'] = PROCESSO_SEM_TELEFONE
    return df


def orquestrar_usuarios_respondidos(df_usuarios, df_usuarios_respondidos):
    if 'CHAVE RELATORIO' not in df_usuarios.columns or 'CHAVE RELATORIO' not in df_usuarios_respondidos.columns:
        return df_usuarios, pd.DataFrame(columns=df_usuarios.columns)

    chave_usuarios = normalizar_texto_serie(df_usuarios['CHAVE RELATORIO'])
    chave_respondidos = normalizar_texto_serie(df_usuarios_respondidos['CHAVE RELATORIO'])
    set_respondidos = set(chave_respondidos[chave_respondidos != ''])

    mask_resolvidos = chave_usuarios.isin(set_respondidos)
    df_usuarios_resolvidos = df_usuarios[mask_resolvidos].copy()
    if len(df_usuarios_resolvidos) > 0:
        if 'PROCESSO' not in df_usuarios_resolvidos.columns:
            df_usuarios_resolvidos['PROCESSO'] = ''
        df_usuarios_resolvidos['PROCESSO'] = PROCESSO_RESOLVIDO
    df_usuarios_restantes = df_usuarios[~mask_resolvidos].copy()
    return df_usuarios_restantes, df_usuarios_resolvidos


def gerar_dataset_final(arquivo_dataset_entrada, arquivo_dataset_saida):
    planilhas = pd.read_excel(arquivo_dataset_entrada, sheet_name=None)

    for aba in ABAS_PADRAO:
        if aba not in planilhas:
            planilhas[aba] = pd.DataFrame()

    usuarios = planilhas['usuarios'].copy()
    usuarios_respondidos = planilhas['usuarios_respondidos'].copy()
    usuarios_resolvidos_existente = planilhas['usuarios_resolvidos'].copy()

    usuarios = aplicar_classificacao_processo_acao(usuarios)
    usuarios_respondidos = aplicar_classificacao_processo_acao(usuarios_respondidos)

    usuarios, usuarios_resolvidos_novos = orquestrar_usuarios_respondidos(
        usuarios, usuarios_respondidos
    )
    if len(usuarios_resolvidos_existente) == 0:
        usuarios_resolvidos = usuarios_resolvidos_novos.copy()
    elif len(usuarios_resolvidos_novos) == 0:
        usuarios_resolvidos = usuarios_resolvidos_existente.copy()
    else:
        usuarios_resolvidos = pd.concat(
            [usuarios_resolvidos_existente, usuarios_resolvidos_novos],
            ignore_index=True,
        )

    planilhas['usuarios'] = usuarios
    planilhas['usuarios_respondidos'] = usuarios_respondidos
    planilhas['usuarios_resolvidos'] = usuarios_resolvidos

    with pd.ExcelWriter(arquivo_dataset_saida, engine='openpyxl') as writer:
        for aba, df in planilhas.items():
            df.to_excel(writer, sheet_name=aba[:31], index=False)

    return {
        'ok': True,
        'arquivo_saida': arquivo_dataset_saida,
        'total_usuarios': len(planilhas['usuarios']),
        'total_usuarios_resolvidos': len(planilhas['usuarios_resolvidos']),
        'mensagens': ['Finalizacao de dataset executada com sucesso.'],
    }
