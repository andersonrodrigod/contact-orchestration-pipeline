import pandas as pd
from src.config.schemas import (
    COLUNAS_FINAIS_DATASET,
    COLUNAS_STATUS_FONTE_DATASET,
    COLUNAS_TELEFONE_DATASET,
)

from src.services.dataset_metricas_service import aplicar_contagens_status
from src.services.normalizacao_services import (
    corrigir_texto_bugado,
    normalizar_colunas_telefone_dataframe,
    normalizar_telefone,
    normalizar_tipos_dataframe,
)
from src.services.schema_service import padronizar_colunas_status_resposta
from src.services.validacao_service import (
    validar_colunas_minimas_status_resposta,
    validar_colunas_origem_dataset_complicacao,
)
from src.utils.arquivos import ler_arquivo_csv

def _normalizar_texto_serie(serie):
    return serie.fillna('').astype(str).str.strip()


def _normalizar_nome_serie(serie):
    return _normalizar_texto_serie(serie).str.upper()


def _limpar_valor_texto(valor):
    if pd.isna(valor):
        return ''
    texto = str(valor).strip()
    texto = corrigir_texto_bugado(texto)
    if texto in {'0', '0.0', 'nan', 'NaN', 'None'}:
        return ''
    return texto


def _limpar_coluna_texto(df, coluna):
    if coluna in df.columns:
        df[coluna] = df[coluna].apply(_limpar_valor_texto)
    return df


def _carregar_status_para_lookup(arquivo_status_integrado):
    df_status = ler_arquivo_csv(arquivo_status_integrado)
    faltando = [col for col in ['DT ENVIO'] if col not in df_status.columns]
    if faltando:
        return {
            'ok': False,
            'mensagens': [f'Colunas obrigatorias ausentes no status integrado: {faltando}'],
        }

    for coluna in COLUNAS_STATUS_FONTE_DATASET:
        if coluna not in df_status.columns:
            df_status[coluna] = ''
    if 'NOME_MANIPULADO' not in df_status.columns:
        if 'Contato' in df_status.columns:
            df_status['NOME_MANIPULADO'] = (
                df_status['Contato'].astype(str).str.split('_', n=1).str[0]
            )
        else:
            df_status['NOME_MANIPULADO'] = ''
    if 'Telefone' not in df_status.columns:
        df_status['Telefone'] = ''

    df_status['Contato'] = _normalizar_texto_serie(df_status['Contato'])
    df_status['NOME_MANIPULADO'] = _normalizar_nome_serie(df_status['NOME_MANIPULADO'])
    df_status['Telefone'] = _normalizar_texto_serie(df_status['Telefone']).apply(normalizar_telefone)
    df_status['DT ENVIO'] = _normalizar_texto_serie(df_status['DT ENVIO'])
    df_status['Status'] = _normalizar_texto_serie(df_status['Status']).apply(_limpar_valor_texto)
    df_status['Respondido'] = _normalizar_texto_serie(df_status['Respondido']).apply(_limpar_valor_texto)
    df_status['RESPOSTA'] = _normalizar_texto_serie(df_status['RESPOSTA']).apply(_limpar_valor_texto)
    df_status['__DT_ENVIO_DATA'] = pd.to_datetime(
        df_status['DT ENVIO'],
        errors='coerce',
        dayfirst=True,
    )

    df_status = df_status.sort_values('__DT_ENVIO_DATA', ascending=False, na_position='last')
    df_status_por_contato = df_status.drop_duplicates(subset=['Contato'], keep='first').copy()
    df_status_por_nome_tel = df_status[
        (df_status['NOME_MANIPULADO'] != '') & (df_status['Telefone'] != '')
    ].drop_duplicates(subset=['NOME_MANIPULADO', 'Telefone'], keep='first').copy()

    df_status_por_contato['DT ENVIO'] = (
        df_status_por_contato['__DT_ENVIO_DATA'].dt.strftime('%d/%m/%Y').fillna('')
    )
    df_status_por_nome_tel['DT ENVIO'] = (
        df_status_por_nome_tel['__DT_ENVIO_DATA'].dt.strftime('%d/%m/%Y').fillna('')
    )

    return {
        'ok': True,
        'df_status_full': df_status,
        'df_status_por_contato': df_status_por_contato,
        'df_status_por_nome_tel': df_status_por_nome_tel,
    }


def _enriquecer_dataset_com_status(
    df_dataset,
    df_status_full,
    df_status_por_contato,
    df_status_por_nome_tel,
):
    df_saida = df_dataset.copy()
    if 'USUARIO' not in df_saida.columns:
        return {
            'ok': False,
            'mensagens': ['Coluna USUARIO nao encontrada no dataset para match com status.'],
        }

    colunas_tel_existentes = [col for col in COLUNAS_TELEFONE_DATASET if col in df_saida.columns]
    if not colunas_tel_existentes:
        return {
            'ok': False,
            'mensagens': ['Nenhuma coluna TELEFONE 1..5 encontrada no dataset para match com status.'],
        }

    df_saida['USUARIO'] = _normalizar_nome_serie(df_saida['USUARIO'])
    if 'CHAVE RELATORIO' not in df_saida.columns:
        df_saida['CHAVE RELATORIO'] = ''
    df_saida['CHAVE RELATORIO'] = _normalizar_texto_serie(df_saida['CHAVE RELATORIO'])

    df_status_por_contato = df_status_por_contato.copy()
    df_status_por_nome_tel = df_status_por_nome_tel.copy()
    df_status_por_nome_tel['NOME_MANIPULADO'] = _normalizar_nome_serie(df_status_por_nome_tel['NOME_MANIPULADO'])
    df_status_por_nome_tel['Telefone'] = _normalizar_texto_serie(
        df_status_por_nome_tel['Telefone']
    ).apply(normalizar_telefone)

    total_dataset = len(df_saida)

    df_saida['ULTIMO STATUS DE ENVIO'] = ''
    df_saida['DT ENVIO'] = ''
    df_saida['RESPOSTA'] = ''
    df_saida['IDENTIFICACAO'] = ''
    df_saida['TELEFONE ENVIADO'] = ''
    df_saida['CHAVE STATUS'] = ''
    df_saida['STATUS CHAVE'] = ''
    df_saida['TELEFONE PRIORIDADE'] = ''
    df_saida['STATUS TELEFONE'] = ''
    for i in range(1, 6):
        col_status_tel = f'TELEFONE STATUS {i}'
        if col_status_tel not in df_saida.columns:
            df_saida[col_status_tel] = ''

    # Match principal: CHAVE RELATORIO -> Contato
    mapa_principal = df_status_por_contato.set_index('Contato')
    mask_principal = df_saida['CHAVE RELATORIO'].isin(mapa_principal.index)
    idx_principal = df_saida.index[mask_principal]
    if len(idx_principal) > 0:
        chave = df_saida.loc[idx_principal, 'CHAVE RELATORIO']
        df_saida.loc[idx_principal, 'ULTIMO STATUS DE ENVIO'] = chave.map(mapa_principal['Status']).fillna('')
        df_saida.loc[idx_principal, 'DT ENVIO'] = chave.map(mapa_principal['DT ENVIO']).fillna('')
        df_saida.loc[idx_principal, 'RESPOSTA'] = chave.map(mapa_principal['RESPOSTA']).fillna('')
        df_saida.loc[idx_principal, 'IDENTIFICACAO'] = chave.map(mapa_principal['Respondido']).fillna('')
        df_saida.loc[idx_principal, 'TELEFONE ENVIADO'] = chave.map(mapa_principal['Telefone']).fillna('')
        df_saida.loc[idx_principal, 'CHAVE STATUS'] = df_saida.loc[idx_principal, 'CHAVE RELATORIO']
        df_saida.loc[idx_principal, 'STATUS CHAVE'] = 'OK'

    # Fallback: NOME_MANIPULADO+Telefone -> USUARIO+qualquer telefone 1..5
    idx_sem_principal = df_saida.index[~mask_principal]
    if len(idx_sem_principal) > 0:
        frames_telefone = []
        for coluna_tel in colunas_tel_existentes:
            df_tel = pd.DataFrame(
                {
                    '__ROW_ID': idx_sem_principal,
                    '__NOME_KEY': df_saida.loc[idx_sem_principal, 'USUARIO'],
                    '__TEL_KEY': _normalizar_texto_serie(
                        df_saida.loc[idx_sem_principal, coluna_tel]
                    ).apply(normalizar_telefone),
                }
            )
            frames_telefone.append(df_tel)

        df_chaves_dataset = pd.concat(frames_telefone, ignore_index=True)
        df_chaves_dataset = df_chaves_dataset[
            (df_chaves_dataset['__NOME_KEY'] != '') & (df_chaves_dataset['__TEL_KEY'] != '')
        ]

        if len(df_chaves_dataset) > 0:
            df_merge = df_chaves_dataset.merge(
                df_status_por_nome_tel[
                    [
                        'NOME_MANIPULADO',
                        'Telefone',
                        'Contato',
                        'Status',
                        'Respondido',
                        'RESPOSTA',
                        'DT ENVIO',
                        '__DT_ENVIO_DATA',
                    ]
                ],
                left_on=['__NOME_KEY', '__TEL_KEY'],
                right_on=['NOME_MANIPULADO', 'Telefone'],
                how='left',
            )
            df_merge = df_merge.sort_values('__DT_ENVIO_DATA', ascending=False, na_position='last')
            df_melhor_match = df_merge.drop_duplicates(subset=['__ROW_ID'], keep='first').copy()
            df_melhor_match = df_melhor_match.set_index('__ROW_ID')
            mask_encontrado = df_melhor_match['Status'].notna()
            idx_fallback = df_melhor_match.index[mask_encontrado]

            if len(idx_fallback) > 0:
                df_saida.loc[idx_fallback, 'ULTIMO STATUS DE ENVIO'] = (
                    df_melhor_match.loc[idx_fallback, 'Status'].fillna('')
                )
                df_saida.loc[idx_fallback, 'DT ENVIO'] = (
                    pd.to_datetime(df_melhor_match.loc[idx_fallback, '__DT_ENVIO_DATA'], errors='coerce')
                    .dt.strftime('%d/%m/%Y')
                    .fillna('')
                )
                df_saida.loc[idx_fallback, 'RESPOSTA'] = (
                    df_melhor_match.loc[idx_fallback, 'RESPOSTA'].fillna('')
                )
                df_saida.loc[idx_fallback, 'IDENTIFICACAO'] = (
                    df_melhor_match.loc[idx_fallback, 'Respondido'].fillna('')
                )
                df_saida.loc[idx_fallback, 'TELEFONE ENVIADO'] = (
                    df_melhor_match.loc[idx_fallback, 'Telefone'].fillna('')
                )
                df_saida.loc[idx_fallback, 'CHAVE STATUS'] = (
                    df_melhor_match.loc[idx_fallback, 'Contato'].fillna('')
                )
                df_saida.loc[idx_fallback, 'STATUS CHAVE'] = 'ERROR'

    total_match = int(_normalizar_texto_serie(df_saida['STATUS CHAVE']).ne('').sum())
    total_sem_match = total_dataset - total_match
    if total_match == 0:
        return {
            'ok': False,
            'mensagens': ['Nenhum match encontrado no principal (CHAVE RELATORIO) nem no fallback.'],
            'total_dataset': total_dataset,
            'total_match': total_match,
            'total_sem_match': total_sem_match,
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

    # Marcacao historica por chave: quais telefones ja foram enviados para cada CHAVE RELATORIO.
    mapa_chave_telefones = (
        df_status_full[(df_status_full['Contato'] != '') & (df_status_full['Telefone'] != '')]
        .groupby('Contato')['Telefone']
        .apply(lambda s: set(s.astype(str)))
        .to_dict()
    )
    chave_relatorio_norm = _normalizar_texto_serie(df_saida['CHAVE RELATORIO'])
    for i, coluna_tel in enumerate(colunas_tel_existentes, start=1):
        coluna_status_tel = f'TELEFONE STATUS {i}'
        tel_dataset_norm = _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone)
        mask_enviado = chave_relatorio_norm.index.to_series().apply(
            lambda idx: (
                chave_relatorio_norm.loc[idx] in mapa_chave_telefones
                and tel_dataset_norm.loc[idx] != ''
                and tel_dataset_norm.loc[idx] in mapa_chave_telefones[chave_relatorio_norm.loc[idx]]
            )
        )
        df_saida.loc[mask_enviado, coluna_status_tel] = 'ENVIADO'

    resultado_contagens = aplicar_contagens_status(df_saida, df_status_full)
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


def concatenar_status_resposta_eletivo_internacao(
    arquivo_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_internacao='src/data/status_resposta_internacao.csv',
    arquivo_saida='src/data/status_resposta_eletivo_internacao.csv',
):
    df_eletivo = ler_arquivo_csv(arquivo_eletivo)
    df_internacao = ler_arquivo_csv(arquivo_internacao)
    total_eletivo = len(df_eletivo)
    total_internacao = len(df_internacao)

    validacao_colunas = validar_colunas_minimas_status_resposta(df_eletivo, df_internacao)
    if not validacao_colunas['ok']:
        return {
            'ok': False,
            'mensagens': validacao_colunas['mensagens'],
            'total_eletivo': total_eletivo,
            'total_internacao': total_internacao,
            'total_concatenado': 0,
        }

    colunas_unificadas = sorted(set(df_eletivo.columns).union(set(df_internacao.columns)))
    df_eletivo = df_eletivo.reindex(columns=colunas_unificadas, fill_value='')
    df_internacao = df_internacao.reindex(columns=colunas_unificadas, fill_value='')

    df_concatenado = pd.concat([df_eletivo, df_internacao], ignore_index=True)
    df_concatenado = padronizar_colunas_status_resposta(df_concatenado)
    df_concatenado.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')
    total_concatenado = len(df_concatenado)

    return {
        'ok': True,
        'mensagens': ['Concatenacao status_resposta_eletivo_internacao executada com sucesso.'],
        'total_eletivo': total_eletivo,
        'total_internacao': total_internacao,
        'total_concatenado': total_concatenado,
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
        'CHAVE': 'CHAVE RELATORIO',
        'STATUS': 'ULTIMO STATUS DE ENVIO',
    }

    for coluna_origem, coluna_destino in mapeamento.items():
        if coluna_origem in df_base.columns:
            df_final[coluna_destino] = df_base[coluna_origem]

    for coluna in COLUNAS_FINAIS_DATASET:
        if coluna not in df_final.columns:
            df_final[coluna] = ''

    df_final = df_final[COLUNAS_FINAIS_DATASET].copy()

    colunas_data = [col for col in ['DT INTERNACAO', 'DT ENVIO'] if col in df_final.columns]
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
    df = ler_arquivo_csv(arquivo_complicacao)
    df.columns = [str(col).strip() for col in df.columns]

    validacao_colunas = validar_colunas_origem_dataset_complicacao(df.columns, contexto=contexto)
    if not validacao_colunas['ok']:
        return {
            'ok': False,
            'mensagens': validacao_colunas['mensagens'],
            'colunas_arquivo': list(df.columns),
            'colunas_faltando': validacao_colunas['colunas_faltando'],
        }

    mask_duplicados = df.duplicated(subset=['COD USUARIO'], keep=False)
    df_duplicados = df[mask_duplicados].copy()
    df_sem_duplicados = df[~mask_duplicados].copy()

    resultado_status = _carregar_status_para_lookup(arquivo_status_integrado)
    if not resultado_status['ok']:
        return {
            'ok': False,
            'mensagens': resultado_status['mensagens'],
        }
    df_status_por_contato = resultado_status['df_status_por_contato']
    df_status_por_nome_tel = resultado_status['df_status_por_nome_tel']
    df_status_full = resultado_status['df_status_full']

    df_usuarios = _montar_df_final_complicacao(df_sem_duplicados)
    resultado_enriquecimento = _enriquecer_dataset_com_status(
        df_usuarios,
        df_status_full,
        df_status_por_contato,
        df_status_por_nome_tel,
    )
    if not resultado_enriquecimento['ok']:
        return {
            'ok': False,
            'mensagens': resultado_enriquecimento['mensagens'],
            'total_dataset': resultado_enriquecimento.get('total_dataset', 0),
            'total_match': resultado_enriquecimento.get('total_match', 0),
            'total_sem_match': resultado_enriquecimento.get('total_sem_match', 0),
        }
    df_usuarios = resultado_enriquecimento['df_enriquecido']
    df_usuarios['__DT_ENVIO_ORDENACAO'] = pd.to_datetime(
        df_usuarios['DT ENVIO'],
        errors='coerce',
        dayfirst=True,
    )
    df_usuarios = df_usuarios.sort_values('__DT_ENVIO_ORDENACAO', ascending=False, na_position='last')
    df_usuarios = df_usuarios.drop(columns=['__DT_ENVIO_ORDENACAO'])

    status_lidos = ['Lida', 'Não quis', 'Obito', 'Óbito']
    if 'STATUS' in df_sem_duplicados.columns:
        df_lidos_base = df_sem_duplicados[df_sem_duplicados['STATUS'].isin(status_lidos)]
    else:
        df_lidos_base = df_sem_duplicados.iloc[0:0]
    df_usuarios_lidos = _montar_df_final_complicacao(df_lidos_base)
    resultado_lidos = _enriquecer_dataset_com_status(
        df_usuarios_lidos,
        df_status_full,
        df_status_por_contato,
        df_status_por_nome_tel,
    )
    if resultado_lidos['ok']:
        df_usuarios_lidos = resultado_lidos['df_enriquecido']

    status_respondidos = ['Obito', 'Óbito', 'Não quis']
    if 'STATUS' in df_sem_duplicados.columns and 'P1' in df_sem_duplicados.columns:
        df_resp_base = df_sem_duplicados[
            df_sem_duplicados['STATUS'].isin(status_respondidos)
            & df_sem_duplicados['P1'].notna()
        ]
    else:
        df_resp_base = df_sem_duplicados.iloc[0:0]
    df_usuarios_respondidos = _montar_df_final_complicacao(df_resp_base)
    resultado_respondidos = _enriquecer_dataset_com_status(
        df_usuarios_respondidos,
        df_status_full,
        df_status_por_contato,
        df_status_por_nome_tel,
    )
    if resultado_respondidos['ok']:
        df_usuarios_respondidos = resultado_respondidos['df_enriquecido']

    df_usuarios_duplicados = _montar_df_final_complicacao(df_duplicados)
    resultado_duplicados = _enriquecer_dataset_com_status(
        df_usuarios_duplicados,
        df_status_full,
        df_status_por_contato,
        df_status_por_nome_tel,
    )
    if resultado_duplicados['ok']:
        df_usuarios_duplicados = resultado_duplicados['df_enriquecido']
    df_usuarios_resolvidos = pd.DataFrame(columns=COLUNAS_FINAIS_DATASET)

    with pd.ExcelWriter(arquivo_saida_dataset, engine='openpyxl') as writer:
        df_usuarios.to_excel(writer, sheet_name='usuarios', index=False)
        df_usuarios_lidos.to_excel(writer, sheet_name='usuarios_lidos', index=False)
        df_usuarios_respondidos.to_excel(writer, sheet_name='usuarios_respondidos', index=False)
        df_usuarios_duplicados.to_excel(writer, sheet_name='usuarios_duplicados', index=False)
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
