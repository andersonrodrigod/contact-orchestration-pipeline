import pandas as pd
from core.error_codes import ERRO_CRIACAO_DATASET
from src.config.schemas import (
    COLUNAS_FINAIS_DATASET,
    COLUNAS_STATUS_FONTE_DATASET,
    COLUNAS_TELEFONE_DATASET,
)

from src.services.dataset_metricas_service import (
    aplicar_contagens_status,
    preparar_contagens_status,
)
from src.services.normalizacao_services import (
    normalizar_colunas_telefone_dataframe,
    normalizar_telefone,
    normalizar_tipos_dataframe,
)
from src.services.texto_service import (
    limpar_coluna_texto as _limpar_coluna_texto,
    limpar_valor_texto as _limpar_valor_texto,
    normalizar_nome_serie as _normalizar_nome_serie,
    normalizar_texto_serie as _normalizar_texto_serie,
    simplificar_texto as _simplificar_texto,
)
from src.services.schema_resposta_service import (
    garantir_contrato_resposta_canonica,
    normalizar_coluna_resposta,
)
from src.services.schema_chave_service import (
    COLUNA_CHAVE_SENHA,
    adicionar_chave_senha,
)
from src.services.validacao_service import validar_colunas_origem_dataset_complicacao
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe

VALOR_SEM_TELEFONE_DISPONIVEL = 'SEM_TELEFONE_DISPONIVEL'
VALOR_SEM_TELEFONE_PRIORIDADE = 'SEM TELEFONE'
STATUS_CHAVE_OK_PRINCIPAL = 'OK_PRINCIPAL'
STATUS_CHAVE_SEM_MATCH = 'SEM_MATCH'


def _ordenar_por_chave_principal(df):
    if COLUNA_CHAVE_SENHA not in df.columns:
        return df
    return df.sort_values(COLUNA_CHAVE_SENHA, ascending=True, na_position='last')


def _carregar_status_para_lookup(arquivo_status_integrado):
    df_status = ler_arquivo_csv(arquivo_status_integrado)
    df_status['__ORDEM_ORIGINAL'] = range(len(df_status))
    for coluna in COLUNAS_STATUS_FONTE_DATASET:
        if coluna not in df_status.columns:
            df_status[coluna] = ''
    df_status = normalizar_coluna_resposta(
        df_status,
        criar_vazia=True,
        remover_alias=True,
    )
    garantir_contrato_resposta_canonica(
        df_status,
        contexto='dataset.status_integrado_pos_carregamento',
    )
    if 'Telefone' not in df_status.columns:
        df_status['Telefone'] = ''
    df_status = adicionar_chave_senha(df_status, ['SENHA', COLUNA_CHAVE_SENHA, 'CHAVE', 'Contato'])

    df_status['Contato'] = _normalizar_texto_serie(df_status['Contato'])
    df_status[COLUNA_CHAVE_SENHA] = _normalizar_texto_serie(
        df_status[COLUNA_CHAVE_SENHA]
    )
    df_status['Telefone'] = _normalizar_texto_serie(df_status['Telefone']).apply(normalizar_telefone)
    df_status['DT ENVIO'] = _normalizar_texto_serie(df_status['DT ENVIO'])
    df_status['Status'] = _normalizar_texto_serie(df_status['Status']).apply(_limpar_valor_texto)
    df_status['Respondido'] = _normalizar_texto_serie(df_status['Respondido']).apply(_limpar_valor_texto)
    df_status['resposta'] = _normalizar_texto_serie(df_status['resposta']).apply(_limpar_valor_texto)

    df_status = df_status.sort_values(
        [COLUNA_CHAVE_SENHA, '__ORDEM_ORIGINAL'],
        ascending=[True, True],
        na_position='last',
    )
    df_status_por_contato = df_status[df_status[COLUNA_CHAVE_SENHA] != ''].drop_duplicates(
        subset=[COLUNA_CHAVE_SENHA],
        keep='last',
    ).copy()

    return {
        'ok': True,
        'df_status_full': df_status,
        'df_status_por_contato': df_status_por_contato,
    }


def _definir_proximo_telefone_disponivel(df_saida, colunas_tel_existentes):
    if 'PROXIMO TELEFONE DISPONIVEL' not in df_saida.columns:
        df_saida['PROXIMO TELEFONE DISPONIVEL'] = ''

    for i, coluna_tel in enumerate(colunas_tel_existentes, start=1):
        coluna_status_tel = f'TELEFONE STATUS {i}'
        if coluna_status_tel not in df_saida.columns:
            continue

        tel_disponivel = _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone)
        status_tel = _normalizar_texto_serie(df_saida[coluna_status_tel]).str.upper()

        # Menor indice vence: primeiro telefone valido com status vazio.
        mask_proximo = (
            (df_saida['PROXIMO TELEFONE DISPONIVEL'] == '')
            & (status_tel == '')
            & (tel_disponivel != '')
        )
        df_saida.loc[mask_proximo, 'PROXIMO TELEFONE DISPONIVEL'] = coluna_tel

    # Se nenhum telefone elegivel foi encontrado, marca explicitamente.
    mask_sem_disponivel = df_saida['PROXIMO TELEFONE DISPONIVEL'] == ''
    df_saida.loc[mask_sem_disponivel, 'PROXIMO TELEFONE DISPONIVEL'] = VALOR_SEM_TELEFONE_DISPONIVEL


def _preencher_telefone_prioridade_padrao(df_saida, colunas_tel_existentes):
    # 1) Se nao houve match por TELEFONE ENVIADO, usa a primeira coluna com status ENVIADO.
    for i, coluna_tel in enumerate(colunas_tel_existentes, start=1):
        coluna_status_tel = f'TELEFONE STATUS {i}'
        if coluna_status_tel not in df_saida.columns:
            continue

        tel_col_norm = _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone)
        status_col = _normalizar_texto_serie(df_saida[coluna_status_tel]).str.upper()
        mask = (
            (_normalizar_texto_serie(df_saida['TELEFONE PRIORIDADE']) == '')
            & (status_col == 'ENVIADO')
            & (tel_col_norm != '')
        )
        df_saida.loc[mask, 'TELEFONE PRIORIDADE'] = coluna_tel

    # 2) Se ainda vazio, usa a primeira coluna que tenha telefone preenchido.
    for coluna_tel in colunas_tel_existentes:
        tel_col_norm = _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone)
        mask = (
            (_normalizar_texto_serie(df_saida['TELEFONE PRIORIDADE']) == '')
            & (tel_col_norm != '')
        )
        df_saida.loc[mask, 'TELEFONE PRIORIDADE'] = coluna_tel

    # 3) Se nao houver nenhum telefone nas colunas 1..5, marca explicitamente.
    mask_sem_prioridade = _normalizar_texto_serie(df_saida['TELEFONE PRIORIDADE']) == ''
    df_saida.loc[mask_sem_prioridade, 'TELEFONE PRIORIDADE'] = VALOR_SEM_TELEFONE_PRIORIDADE


def _enriquecer_dataset_com_status(
    df_dataset,
    df_status_full,
    df_status_por_contato,
    contagens_status_preparadas=None,
):
    df_saida = df_dataset.copy()
    if 'USUARIO' not in df_saida.columns:
        return {
            'ok': False,
            'mensagens': ['Coluna USUARIO nao encontrada no dataset para match com status.'],
            'codigo_erro': ERRO_CRIACAO_DATASET,
        }

    colunas_tel_existentes = [col for col in COLUNAS_TELEFONE_DATASET if col in df_saida.columns]
    if not colunas_tel_existentes:
        return {
            'ok': False,
            'mensagens': ['Nenhuma coluna TELEFONE 1..5 encontrada no dataset para match com status.'],
            'codigo_erro': ERRO_CRIACAO_DATASET,
        }

    df_saida['USUARIO'] = _normalizar_nome_serie(df_saida['USUARIO'])
    if 'CHAVE RELATORIO' not in df_saida.columns:
        df_saida['CHAVE RELATORIO'] = ''
    df_saida['CHAVE RELATORIO'] = _normalizar_texto_serie(df_saida['CHAVE RELATORIO'])
    df_saida = adicionar_chave_senha(
        df_saida,
        ['SENHA', COLUNA_CHAVE_SENHA],
    )
    df_saida[COLUNA_CHAVE_SENHA] = _normalizar_texto_serie(
        df_saida[COLUNA_CHAVE_SENHA]
    )

    df_status_por_contato = df_status_por_contato.copy()
    total_dataset = len(df_saida)

    df_saida['ULTIMO STATUS DE ENVIO'] = ''
    df_saida['DT ENVIO'] = ''
    df_saida['RESPOSTA'] = ''
    df_saida['IDENTIFICACAO'] = ''
    df_saida['TELEFONE ENVIADO'] = ''
    df_saida['CHAVE STATUS'] = ''
    df_saida['STATUS CHAVE'] = STATUS_CHAVE_SEM_MATCH
    df_saida['TELEFONE PRIORIDADE'] = ''
    df_saida['PROXIMO TELEFONE DISPONIVEL'] = ''
    df_saida['STATUS TELEFONE'] = ''
    for i in range(1, 6):
        col_status_tel = f'TELEFONE STATUS {i}'
        if col_status_tel not in df_saida.columns:
            df_saida[col_status_tel] = ''

    # Match principal: CHAVE_SENHA -> CHAVE_SENHA
    mapa_principal = df_status_por_contato.set_index(COLUNA_CHAVE_SENHA)
    mask_principal = df_saida[COLUNA_CHAVE_SENHA].isin(mapa_principal.index)
    idx_principal = df_saida.index[mask_principal]
    if len(idx_principal) > 0:
        chave = df_saida.loc[idx_principal, COLUNA_CHAVE_SENHA]
        df_saida.loc[idx_principal, 'ULTIMO STATUS DE ENVIO'] = chave.map(mapa_principal['Status']).fillna('')
        df_saida.loc[idx_principal, 'DT ENVIO'] = chave.map(mapa_principal['DT ENVIO']).fillna('')
        df_saida.loc[idx_principal, 'RESPOSTA'] = chave.map(mapa_principal['resposta']).fillna('')
        df_saida.loc[idx_principal, 'IDENTIFICACAO'] = chave.map(mapa_principal['Respondido']).fillna('')
        df_saida.loc[idx_principal, 'TELEFONE ENVIADO'] = chave.map(mapa_principal['Telefone']).fillna('')
        df_saida.loc[idx_principal, 'CHAVE STATUS'] = df_saida.loc[
            idx_principal,
            COLUNA_CHAVE_SENHA,
        ]
        df_saida.loc[idx_principal, 'STATUS CHAVE'] = STATUS_CHAVE_OK_PRINCIPAL

    serie_status_chave = _normalizar_texto_serie(df_saida['STATUS CHAVE'])
    total_match = int((serie_status_chave == STATUS_CHAVE_OK_PRINCIPAL).sum())
    total_sem_match = int((serie_status_chave == STATUS_CHAVE_SEM_MATCH).sum())
    if total_match == 0:
        return {
            'ok': False,
            'mensagens': ['Nenhum match encontrado por CHAVE_SENHA.'],
            'total_dataset': total_dataset,
            'total_match': total_match,
            'total_sem_match': total_sem_match,
            'codigo_erro': ERRO_CRIACAO_DATASET,
        }

    # TELEFONE PRIORIDADE recebe o nome da coluna onde bate com TELEFONE ENVIADO
    telefone_enviado_norm = _normalizar_texto_serie(df_saida['TELEFONE ENVIADO']).apply(normalizar_telefone)
    match_telefone_any = pd.Series(False, index=df_saida.index)
    for coluna_tel in colunas_tel_existentes:
        tel_col_norm = _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone)
        match_col = (telefone_enviado_norm != '') & (tel_col_norm == telefone_enviado_norm)
        match_telefone_any = match_telefone_any | match_col
        mask_tel = (
            (df_saida['TELEFONE PRIORIDADE'] == '')
            & match_col
        )
        df_saida.loc[mask_tel, 'TELEFONE PRIORIDADE'] = coluna_tel
    df_saida.loc[match_telefone_any, 'STATUS TELEFONE'] = 'OK'
    df_saida.loc[~match_telefone_any, 'STATUS TELEFONE'] = 'ERROR'

    # Marcacao historica por chave: quais telefones ja foram enviados para cada CHAVE_SENHA.
    mapa_chave_telefones = (
        df_status_full[
            (df_status_full[COLUNA_CHAVE_SENHA] != '')
            & (df_status_full['Telefone'] != '')
        ]
        .groupby(COLUNA_CHAVE_SENHA)['Telefone']
        .apply(lambda s: set(s.astype(str)))
        .to_dict()
    )
    chave_principal_norm = _normalizar_texto_serie(df_saida[COLUNA_CHAVE_SENHA])
    for i, coluna_tel in enumerate(colunas_tel_existentes, start=1):
        coluna_status_tel = f'TELEFONE STATUS {i}'
        tel_dataset_norm = _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone)
        mask_enviado = chave_principal_norm.index.to_series().apply(
            lambda idx: (
                chave_principal_norm.loc[idx] in mapa_chave_telefones
                and tel_dataset_norm.loc[idx] != ''
                and tel_dataset_norm.loc[idx] in mapa_chave_telefones[chave_principal_norm.loc[idx]]
            )
        )
        df_saida.loc[mask_enviado, coluna_status_tel] = 'ENVIADO'

    _preencher_telefone_prioridade_padrao(df_saida, colunas_tel_existentes)
    _definir_proximo_telefone_disponivel(df_saida, colunas_tel_existentes)

    resultado_contagens = aplicar_contagens_status(
        df_saida,
        df_status_full,
        contagens_preparadas=contagens_status_preparadas,
    )
    if not resultado_contagens['ok']:
        return resultado_contagens

    for coluna in [
        'ULTIMO STATUS DE ENVIO',
        'DT ENVIO',
        'RESPOSTA',
        'IDENTIFICACAO',
        'TELEFONE ENVIADO',
        'CHAVE STATUS',
        'STATUS CHAVE',
        'TELEFONE PRIORIDADE',
        'PROXIMO TELEFONE DISPONIVEL',
        'STATUS TELEFONE',
        'TELEFONE STATUS 1',
        'TELEFONE STATUS 2',
        'TELEFONE STATUS 3',
        'TELEFONE STATUS 4',
        'TELEFONE STATUS 5',
    ]:
        _limpar_coluna_texto(df_saida, coluna)

    qtd_identificacao = int(_normalizar_texto_serie(df_saida['IDENTIFICACAO']).ne('').sum())
    qtd_resposta = int(_normalizar_texto_serie(df_saida['RESPOSTA']).ne('').sum())
    pct_identificacao = (qtd_identificacao / total_dataset) * 100 if total_dataset > 0 else 0.0
    pct_resposta = (qtd_resposta / total_dataset) * 100 if total_dataset > 0 else 0.0

    serie_status = _normalizar_texto_serie(df_saida['ULTIMO STATUS DE ENVIO'])
    serie_status_valida = serie_status[serie_status != '']
    dist_status = {}
    for valor, qtd in serie_status_valida.value_counts().to_dict().items():
        dist_status[valor] = {
            'qtd': int(qtd),
            'pct': (int(qtd) / total_dataset) * 100 if total_dataset > 0 else 0.0,
        }

    return {
        'ok': True,
        'df_enriquecido': df_saida,
        'total_dataset': total_dataset,
        'total_match': total_match,
        'total_sem_match': total_sem_match,
        'qtd_identificacao': qtd_identificacao,
        'pct_identificacao': pct_identificacao,
        'qtd_resposta': qtd_resposta,
        'pct_resposta': pct_resposta,
        'distribuicao_status': dist_status,
    }


def concatenar_arquivos_livre(
    arquivo_a,
    arquivo_b,
    arquivo_saida,
):
    """Concatena dois arquivos usando uniao de colunas, sem validar schema minimo."""
    df_a = ler_arquivo_csv(arquivo_a)
    df_b = ler_arquivo_csv(arquivo_b)

    total_a = len(df_a)
    total_b = len(df_b)

    colunas_a = set(df_a.columns)
    colunas_b = set(df_b.columns)
    colunas_unificadas = sorted(colunas_a.union(colunas_b))
    colunas_comuns = sorted(colunas_a.intersection(colunas_b))
    colunas_apenas_a = sorted(colunas_a - colunas_b)
    colunas_apenas_b = sorted(colunas_b - colunas_a)

    df_a = df_a.reindex(columns=colunas_unificadas, fill_value='')
    df_b = df_b.reindex(columns=colunas_unificadas, fill_value='')
    df_concatenado = pd.concat([df_a, df_b], ignore_index=True)

    salvar_dataframe(df_concatenado, arquivo_saida)

    mensagens = [
        'Concatenacao livre executada com sucesso.',
        f'Colunas em comum: {len(colunas_comuns)}.',
        f'Colunas apenas no arquivo A: {len(colunas_apenas_a)}.',
        f'Colunas apenas no arquivo B: {len(colunas_apenas_b)}.',
    ]

    return {
        'ok': True,
        'mensagens': mensagens,
        'total_arquivo_a': total_a,
        'total_arquivo_b': total_b,
        'total_concatenado': len(df_concatenado),
        'total_colunas': len(colunas_unificadas),
        'colunas_comuns': colunas_comuns,
        'colunas_apenas_a': colunas_apenas_a,
        'colunas_apenas_b': colunas_apenas_b,
        'arquivo_saida': arquivo_saida,
    }


def _montar_df_final_complicacao(df_base):
    df_final = pd.DataFrame(index=df_base.index)

    mapeamento = {
        'BASE': 'BASE',
        'COD USUARIO': 'COD USUARIO',
        'USUARIO': 'USUARIO',
        'TELEFONE 1': 'TELEFONE 1',
        'TELEFONE 2': 'TELEFONE 2',
        'TELEFONE 3': 'TELEFONE 3',
        'TELEFONE 4': 'TELEFONE 4',
        'TELEFONE 5': 'TELEFONE 5',
        'PRESTADOR': 'PRESTADOR',
        'PROCEDIMENTO': 'PROCEDIMENTO',
        'TP ATENDIMENTO': 'TP ATENDIMENTO',
        'DT INTERNACAO': 'DT INTERNACAO',
        'DT ENVIO': 'DT ENVIO',
        'SENHA': COLUNA_CHAVE_SENHA,
        'CHAVE': 'CHAVE RELATORIO',
        'STATUS': 'ULTIMO STATUS DE ENVIO',
    }

    for coluna_origem, coluna_destino in mapeamento.items():
        if coluna_origem in df_base.columns:
            df_final[coluna_destino] = df_base[coluna_origem]

    df_final = adicionar_chave_senha(
        df_final,
        [COLUNA_CHAVE_SENHA],
    )

    for coluna in COLUNAS_FINAIS_DATASET:
        if coluna not in df_final.columns:
            df_final[coluna] = ''

    df_final = df_final[COLUNAS_FINAIS_DATASET].copy()

    # DT INTERNACAO e informativa: nao forcar parse para data evita perder texto invalido.
    colunas_data = [col for col in ['DT ENVIO'] if col in df_final.columns]
    df_final = normalizar_tipos_dataframe(df_final, colunas_data=colunas_data)
    colunas_telefone = [col for col in df_final.columns if 'TELEFONE' in col]
    df_final = normalizar_colunas_telefone_dataframe(df_final, colunas_telefone)

    return df_final


def criar_dataset_complicacao(
    arquivo_complicacao,
    arquivo_saida_dataset,
    arquivo_status_integrado,
    contexto='dataset',
):
    etapa_atual = 'INICIO'
    try:
        etapa_atual = 'LEITURA_ARQUIVO_COMPLICACAO'
        df = ler_arquivo_csv(arquivo_complicacao)
        df.columns = [str(col).strip() for col in df.columns]

        etapa_atual = 'VALIDACAO_COLUNAS_ORIGEM'
        validacao_colunas = validar_colunas_origem_dataset_complicacao(df.columns, contexto=contexto)
        if not validacao_colunas['ok']:
            return {
                'ok': False,
                'mensagens': validacao_colunas['mensagens'],
                'colunas_arquivo': list(df.columns),
                'colunas_faltando': validacao_colunas['colunas_faltando'],
                'colunas_duplicadas': validacao_colunas.get('colunas_duplicadas', []),
                'colunas_mascaradas_duplicadas': validacao_colunas.get(
                    'colunas_mascaradas_duplicadas', []
                ),
                'codigo_erro': ERRO_CRIACAO_DATASET,
            }

        etapa_atual = 'VALIDACAO_SEGMENTACAO'
        colunas_criticas_segmentacao = ['STATUS', 'P1']
        colunas_criticas_faltando = [c for c in colunas_criticas_segmentacao if c not in df.columns]
        if len(colunas_criticas_faltando) > 0:
            return {
                'ok': False,
                'mensagens': [
                    'Estrutura de segmentacao invalida para abas secundarias.',
                    f'Colunas obrigatorias ausentes: {colunas_criticas_faltando}',
                    'Ajuste os nomes das colunas para STATUS e P1 no arquivo de origem.',
                ],
                'colunas_arquivo': list(df.columns),
                'colunas_faltando': colunas_criticas_faltando,
                'codigo_erro': ERRO_CRIACAO_DATASET,
            }

        etapa_atual = 'SEPARACAO_DUPLICADOS'
        # Mantem todos os registros na aba usuarios (nao separar mais em usuarios_duplicados).
        df_sem_duplicados = df.copy()

        etapa_atual = 'CARREGAR_STATUS_LOOKUP'
        resultado_status = _carregar_status_para_lookup(arquivo_status_integrado)
        if not resultado_status['ok']:
            return {
                'ok': False,
                'mensagens': resultado_status['mensagens'],
                'codigo_erro': resultado_status.get('codigo_erro', ERRO_CRIACAO_DATASET),
            }
        df_status_por_contato = resultado_status['df_status_por_contato']
        df_status_full = resultado_status['df_status_full']
        resultado_contagens_preparadas = preparar_contagens_status(df_status_full)
        if not resultado_contagens_preparadas['ok']:
            return {
                'ok': False,
                'mensagens': resultado_contagens_preparadas['mensagens'],
                'codigo_erro': ERRO_CRIACAO_DATASET,
            }
        contagens_status_preparadas = resultado_contagens_preparadas

        etapa_atual = 'ENRIQUECER_ABA_PRINCIPAL'
        df_usuarios = _montar_df_final_complicacao(df_sem_duplicados)
        resultado_enriquecimento = _enriquecer_dataset_com_status(
            df_usuarios,
            df_status_full,
            df_status_por_contato,
            contagens_status_preparadas=contagens_status_preparadas,
        )
        if not resultado_enriquecimento['ok']:
            return {
                'ok': False,
                'mensagens': resultado_enriquecimento['mensagens'],
                'total_dataset': resultado_enriquecimento.get('total_dataset', 0),
                'total_match': resultado_enriquecimento.get('total_match', 0),
                'total_sem_match': resultado_enriquecimento.get('total_sem_match', 0),
                'codigo_erro': resultado_enriquecimento.get('codigo_erro', ERRO_CRIACAO_DATASET),
            }
        df_usuarios = resultado_enriquecimento['df_enriquecido']
        df_usuarios = _ordenar_por_chave_principal(df_usuarios)

        etapa_atual = 'MONTAR_ABAS_SECUNDARIAS'
        if 'P1' in df_sem_duplicados.columns:
            p1_preenchido = _normalizar_texto_serie(df_sem_duplicados['P1']) != ''
            if 'STATUS' in df_sem_duplicados.columns:
                status_respondidos = {'obito', 'nao quis'}
                status_norm = df_sem_duplicados['STATUS'].apply(_simplificar_texto)
                mask_respondidos = p1_preenchido | status_norm.isin(status_respondidos)
            else:
                mask_respondidos = p1_preenchido
            df_resp_base = df_sem_duplicados[mask_respondidos]
        elif 'STATUS' in df_sem_duplicados.columns:
            status_respondidos = {'obito', 'nao quis'}
            status_norm = df_sem_duplicados['STATUS'].apply(_simplificar_texto)
            df_resp_base = df_sem_duplicados[status_norm.isin(status_respondidos)]
        else:
            df_resp_base = df_sem_duplicados.iloc[0:0]
        if len(df_resp_base) == 0:
            return {
                'ok': False,
                'mensagens': [
                    'Nenhum usuario respondido encontrado para montar a aba usuarios_respondidos.',
                    'Fluxo interrompido para evitar geracao silenciosa de aba vazia em cenário de respondidos.',
                ],
                'codigo_erro': ERRO_CRIACAO_DATASET,
            }

        etapa_atual = 'ENRIQUECER_USUARIOS_RESPONDIDOS'
        df_usuarios_respondidos = _montar_df_final_complicacao(df_resp_base)
        resultado_respondidos = _enriquecer_dataset_com_status(
            df_usuarios_respondidos,
            df_status_full,
            df_status_por_contato,
            contagens_status_preparadas=contagens_status_preparadas,
        )
        if not resultado_respondidos['ok']:
            return {
                'ok': False,
                'mensagens': resultado_respondidos['mensagens'],
                'total_dataset': resultado_respondidos.get('total_dataset', len(df_resp_base)),
                'total_match': resultado_respondidos.get('total_match', 0),
                'total_sem_match': resultado_respondidos.get('total_sem_match', 0),
                'codigo_erro': resultado_respondidos.get('codigo_erro', ERRO_CRIACAO_DATASET),
            }
        df_usuarios_respondidos = resultado_respondidos['df_enriquecido']
        df_usuarios_respondidos = _ordenar_por_chave_principal(df_usuarios_respondidos)

        etapa_atual = 'ENRIQUECER_USUARIOS_DUPLICADOS'
        # Aba de duplicados foi descontinuada por regra de negocio.
        df_usuarios_duplicados = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)
        df_usuarios_resolvidos = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)

        etapa_atual = 'PERSISTENCIA_XLSX'
        with pd.ExcelWriter(arquivo_saida_dataset, engine='openpyxl') as writer:
            df_usuarios.to_excel(writer, sheet_name='usuarios', index=False)
            df_usuarios_respondidos.to_excel(writer, sheet_name='usuarios_respondidos', index=False)
            df_usuarios_resolvidos.to_excel(writer, sheet_name='usuarios_resolvidos', index=False)

        return {
            'ok': True,
            'arquivo_saida': arquivo_saida_dataset,
            'total_linhas': len(df_usuarios),
            'total_dataset': resultado_enriquecimento['total_dataset'],
            'total_match': resultado_enriquecimento['total_match'],
            'total_sem_match': resultado_enriquecimento['total_sem_match'],
            'qtd_identificacao': resultado_enriquecimento['qtd_identificacao'],
            'pct_identificacao': resultado_enriquecimento['pct_identificacao'],
            'qtd_resposta': resultado_enriquecimento['qtd_resposta'],
            'pct_resposta': resultado_enriquecimento['pct_resposta'],
            'distribuicao_status': resultado_enriquecimento['distribuicao_status'],
            'mensagens': validacao_colunas['mensagens'] + [f'Dataset de {contexto} criado com sucesso.'],
            'colunas_arquivo': list(df.columns),
            'colunas_faltando': [],
        }
    except Exception as erro:
        return {
            'ok': False,
            'mensagens': [
                (
                    f'Erro inesperado na criacao do dataset (etapa={etapa_atual}): '
                    f'{type(erro).__name__}: {erro}'
                )
            ],
            'codigo_erro': ERRO_CRIACAO_DATASET,
        }


def criar_dataset_base_complicacao(
    arquivo_complicacao,
    arquivo_saida_dataset,
    contexto='dataset_base',
):
    """Cria dataset base sem integrar status/resposta."""
    etapa_atual = 'INICIO'
    try:
        etapa_atual = 'LEITURA_ARQUIVO_COMPLICACAO'
        df = ler_arquivo_csv(arquivo_complicacao)
        df.columns = [str(col).strip() for col in df.columns]

        etapa_atual = 'VALIDACAO_COLUNAS_ORIGEM'
        validacao_colunas = validar_colunas_origem_dataset_complicacao(df.columns, contexto=contexto)
        if not validacao_colunas['ok']:
            return {
                'ok': False,
                'mensagens': validacao_colunas['mensagens'],
                'colunas_arquivo': list(df.columns),
                'colunas_faltando': validacao_colunas['colunas_faltando'],
                'colunas_duplicadas': validacao_colunas.get('colunas_duplicadas', []),
                'colunas_mascaradas_duplicadas': validacao_colunas.get(
                    'colunas_mascaradas_duplicadas', []
                ),
                'codigo_erro': ERRO_CRIACAO_DATASET,
            }

        etapa_atual = 'MONTAGEM_ABAS_BASE'
        df_sem_duplicados = df.copy()
        df_usuarios = _montar_df_final_complicacao(df_sem_duplicados)

        if 'P1' in df_sem_duplicados.columns:
            p1_preenchido = _normalizar_texto_serie(df_sem_duplicados['P1']) != ''
            if 'STATUS' in df_sem_duplicados.columns:
                status_respondidos = {'obito', 'nao quis'}
                status_norm = df_sem_duplicados['STATUS'].apply(_simplificar_texto)
                mask_respondidos = p1_preenchido | status_norm.isin(status_respondidos)
            else:
                mask_respondidos = p1_preenchido
            df_resp_base = df_sem_duplicados[mask_respondidos]
        elif 'STATUS' in df_sem_duplicados.columns:
            status_respondidos = {'obito', 'nao quis'}
            status_norm = df_sem_duplicados['STATUS'].apply(_simplificar_texto)
            df_resp_base = df_sem_duplicados[status_norm.isin(status_respondidos)]
        else:
            df_resp_base = df_sem_duplicados.iloc[0:0]

        df_usuarios_respondidos = _montar_df_final_complicacao(df_resp_base)
        df_usuarios_resolvidos = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)

        etapa_atual = 'PERSISTENCIA_XLSX'
        with pd.ExcelWriter(arquivo_saida_dataset, engine='openpyxl') as writer:
            df_usuarios.to_excel(writer, sheet_name='usuarios', index=False)
            df_usuarios_respondidos.to_excel(writer, sheet_name='usuarios_respondidos', index=False)
            df_usuarios_resolvidos.to_excel(writer, sheet_name='usuarios_resolvidos', index=False)

        return {
            'ok': True,
            'arquivo_saida': arquivo_saida_dataset,
            'total_linhas': len(df_usuarios),
            'mensagens': validacao_colunas['mensagens'] + [f'Dataset base de {contexto} criado com sucesso.'],
            'colunas_arquivo': list(df.columns),
            'colunas_faltando': [],
        }
    except Exception as erro:
        return {
            'ok': False,
            'mensagens': [
                (
                    f'Erro inesperado na criacao do dataset base (etapa={etapa_atual}): '
                    f'{type(erro).__name__}: {erro}'
                )
            ],
            'codigo_erro': ERRO_CRIACAO_DATASET,
        }


def aplicar_status_integrado_em_dataset(
    arquivo_dataset_base,
    arquivo_status_integrado,
    arquivo_saida_dataset,
    contexto='dataset_com_status',
):
    """Aplica status integrado em dataset base previamente criado."""
    etapa_atual = 'INICIO'
    try:
        etapa_atual = 'LEITURA_DATASET_BASE'
        if str(arquivo_dataset_base).lower().endswith(('.xlsx', '.xls')):
            abas = pd.read_excel(arquivo_dataset_base, sheet_name=None, dtype=str)
            df_usuarios = abas.get('usuarios', pd.DataFrame()).fillna('')
            df_usuarios_respondidos = abas.get('usuarios_respondidos', pd.DataFrame()).fillna('')
            df_usuarios_resolvidos = abas.get('usuarios_resolvidos', pd.DataFrame()).fillna('')
        else:
            df_usuarios = ler_arquivo_csv(arquivo_dataset_base)
            df_usuarios_respondidos = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)
            df_usuarios_resolvidos = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)

        etapa_atual = 'CARREGAR_STATUS_LOOKUP'
        resultado_status = _carregar_status_para_lookup(arquivo_status_integrado)
        if not resultado_status['ok']:
            return {
                'ok': False,
                'mensagens': resultado_status['mensagens'],
                'codigo_erro': resultado_status.get('codigo_erro', ERRO_CRIACAO_DATASET),
            }
        df_status_por_contato = resultado_status['df_status_por_contato']
        df_status_full = resultado_status['df_status_full']

        resultado_contagens_preparadas = preparar_contagens_status(df_status_full)
        if not resultado_contagens_preparadas['ok']:
            return {
                'ok': False,
                'mensagens': resultado_contagens_preparadas['mensagens'],
                'codigo_erro': ERRO_CRIACAO_DATASET,
            }
        contagens_status_preparadas = resultado_contagens_preparadas

        etapa_atual = 'ENRIQUECER_ABA_USUARIOS'
        resultado_usuarios = _enriquecer_dataset_com_status(
            df_usuarios,
            df_status_full,
            df_status_por_contato,
            contagens_status_preparadas=contagens_status_preparadas,
        )
        if not resultado_usuarios['ok']:
            return {
                'ok': False,
                'mensagens': resultado_usuarios['mensagens'],
                'total_dataset': resultado_usuarios.get('total_dataset', 0),
                'total_match': resultado_usuarios.get('total_match', 0),
                'total_sem_match': resultado_usuarios.get('total_sem_match', 0),
                'codigo_erro': resultado_usuarios.get('codigo_erro', ERRO_CRIACAO_DATASET),
            }
        df_usuarios_out = _ordenar_por_chave_principal(resultado_usuarios['df_enriquecido'])

        etapa_atual = 'ENRIQUECER_ABAS_SECUNDARIAS'
        if len(df_usuarios_respondidos) > 0:
            resultado_respondidos = _enriquecer_dataset_com_status(
                df_usuarios_respondidos,
                df_status_full,
                df_status_por_contato,
                contagens_status_preparadas=contagens_status_preparadas,
            )
            if resultado_respondidos['ok']:
                df_usuarios_respondidos = _ordenar_por_chave_principal(
                    resultado_respondidos['df_enriquecido']
                )
        if len(df_usuarios_resolvidos) > 0:
            resultado_resolvidos = _enriquecer_dataset_com_status(
                df_usuarios_resolvidos,
                df_status_full,
                df_status_por_contato,
                contagens_status_preparadas=contagens_status_preparadas,
            )
            if resultado_resolvidos['ok']:
                df_usuarios_resolvidos = _ordenar_por_chave_principal(
                    resultado_resolvidos['df_enriquecido']
                )

        etapa_atual = 'PERSISTENCIA_XLSX'
        with pd.ExcelWriter(arquivo_saida_dataset, engine='openpyxl') as writer:
            df_usuarios_out.to_excel(writer, sheet_name='usuarios', index=False)
            df_usuarios_respondidos.to_excel(writer, sheet_name='usuarios_respondidos', index=False)
            df_usuarios_resolvidos.to_excel(writer, sheet_name='usuarios_resolvidos', index=False)

        return {
            'ok': True,
            'arquivo_saida': arquivo_saida_dataset,
            'total_linhas': len(df_usuarios_out),
            'total_dataset': resultado_usuarios['total_dataset'],
            'total_match': resultado_usuarios['total_match'],
            'total_sem_match': resultado_usuarios['total_sem_match'],
            'qtd_identificacao': resultado_usuarios['qtd_identificacao'],
            'pct_identificacao': resultado_usuarios['pct_identificacao'],
            'qtd_resposta': resultado_usuarios['qtd_resposta'],
            'pct_resposta': resultado_usuarios['pct_resposta'],
            'distribuicao_status': resultado_usuarios['distribuicao_status'],
            'mensagens': [f'Status integrado aplicado no {contexto} com sucesso.'],
        }
    except Exception as erro:
        return {
            'ok': False,
            'mensagens': [
                (
                    f'Erro inesperado ao aplicar status no dataset (etapa={etapa_atual}): '
                    f'{type(erro).__name__}: {erro}'
                )
            ],
            'codigo_erro': ERRO_CRIACAO_DATASET,
        }


