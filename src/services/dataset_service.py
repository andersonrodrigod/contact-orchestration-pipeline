import pandas as pd
from src.config.schemas import COLUNAS_FINAIS_DATASET

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


COLUNAS_STATUS_PARA_DATASET = ['Contato', 'DT ENVIO', 'Status', 'Respondido', 'RESPOSTA']
COLUNAS_TELEFONE_DATASET = ['TELEFONE 1', 'TELEFONE 2', 'TELEFONE 3', 'TELEFONE 4', 'TELEFONE 5']


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

    for coluna in COLUNAS_STATUS_PARA_DATASET:
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

    df_status = df_status[df_status['NOME_MANIPULADO'] != '']
    df_status = df_status[df_status['Telefone'] != '']
    df_status = df_status.sort_values('__DT_ENVIO_DATA', ascending=False, na_position='last')
    df_status_ultimo = df_status.drop_duplicates(subset=['NOME_MANIPULADO', 'Telefone'], keep='first').copy()
    df_status_ultimo['DT ENVIO'] = df_status_ultimo['__DT_ENVIO_DATA'].dt.strftime('%d/%m/%Y').fillna('')

    return {'ok': True, 'df_status_ultimo': df_status_ultimo}


def _enriquecer_dataset_com_status(df_dataset, df_status_ultimo):
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
    df_status_ultimo = df_status_ultimo.copy()
    df_status_ultimo['NOME_MANIPULADO'] = _normalizar_nome_serie(df_status_ultimo['NOME_MANIPULADO'])
    df_status_ultimo['Telefone'] = _normalizar_texto_serie(df_status_ultimo['Telefone']).apply(
        normalizar_telefone
    )

    frames_telefone = []
    for coluna_tel in colunas_tel_existentes:
        df_tel = pd.DataFrame(
            {
                '__ROW_ID': df_saida.index,
                '__NOME_KEY': df_saida['USUARIO'],
                '__TEL_KEY': _normalizar_texto_serie(df_saida[coluna_tel]).apply(normalizar_telefone),
            }
        )
        frames_telefone.append(df_tel)

    df_chaves_dataset = pd.concat(frames_telefone, ignore_index=True)
    df_chaves_dataset = df_chaves_dataset[
        (df_chaves_dataset['__NOME_KEY'] != '') & (df_chaves_dataset['__TEL_KEY'] != '')
    ]
    total_dataset = len(df_saida)
    if len(df_chaves_dataset) == 0:
        return {
            'ok': False,
            'mensagens': ['Dataset sem combinacao valida de USUARIO + TELEFONE 1..5 para match.'],
            'total_dataset': total_dataset,
            'total_match': 0,
            'total_sem_match': total_dataset,
        }

    df_merge = df_chaves_dataset.merge(
        df_status_ultimo[
            ['NOME_MANIPULADO', 'Telefone', 'Status', 'Respondido', 'RESPOSTA', 'DT ENVIO', '__DT_ENVIO_DATA']
        ],
        left_on=['__NOME_KEY', '__TEL_KEY'],
        right_on=['NOME_MANIPULADO', 'Telefone'],
        how='left',
    )
    df_merge = df_merge.sort_values('__DT_ENVIO_DATA', ascending=False, na_position='last')
    df_melhor_match = df_merge.drop_duplicates(subset=['__ROW_ID'], keep='first').copy()
    df_melhor_match = df_melhor_match.set_index('__ROW_ID')

    mask_encontrado = df_melhor_match['Status'].notna()
    total_match = int(mask_encontrado.sum())
    total_sem_match = total_dataset - total_match

    if total_match == 0:
        return {
            'ok': False,
            'mensagens': [
                'Nenhum match encontrado entre NOME_MANIPULADO+Telefone (status) e USUARIO+TELEFONE 1..5 (dataset).'
            ],
            'total_dataset': total_dataset,
            'total_match': total_match,
            'total_sem_match': total_sem_match,
        }

    df_saida['ULTIMO STATUS DE ENVIO'] = ''
    df_saida['DT ENVIO'] = ''
    df_saida['RESPOSTA'] = ''
    df_saida['IDENTIFICACAO'] = ''
    df_saida['TELEFONE ENVIADO'] = ''

    idx_match = df_melhor_match.index[mask_encontrado]
    df_saida.loc[idx_match, 'ULTIMO STATUS DE ENVIO'] = df_melhor_match.loc[idx_match, 'Status'].fillna('')
    df_saida.loc[idx_match, 'DT ENVIO'] = (
        pd.to_datetime(df_melhor_match.loc[idx_match, '__DT_ENVIO_DATA'], errors='coerce')
        .dt.strftime('%d/%m/%Y')
        .fillna('')
    )
    df_saida.loc[idx_match, 'RESPOSTA'] = df_melhor_match.loc[idx_match, 'RESPOSTA'].fillna('')
    df_saida.loc[idx_match, 'IDENTIFICACAO'] = df_melhor_match.loc[idx_match, 'Respondido'].fillna('')
    df_saida.loc[idx_match, 'TELEFONE ENVIADO'] = df_melhor_match.loc[idx_match, 'Telefone'].fillna('')

    for coluna in ['ULTIMO STATUS DE ENVIO', 'DT ENVIO', 'RESPOSTA', 'IDENTIFICACAO', 'TELEFONE ENVIADO']:
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
    df_status_ultimo = resultado_status['df_status_ultimo']

    df_usuarios = _montar_df_final_complicacao(df_sem_duplicados)
    resultado_enriquecimento = _enriquecer_dataset_com_status(df_usuarios, df_status_ultimo)
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
    resultado_lidos = _enriquecer_dataset_com_status(df_usuarios_lidos, df_status_ultimo)
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
    resultado_respondidos = _enriquecer_dataset_com_status(df_usuarios_respondidos, df_status_ultimo)
    if resultado_respondidos['ok']:
        df_usuarios_respondidos = resultado_respondidos['df_enriquecido']

    df_usuarios_duplicados = _montar_df_final_complicacao(df_duplicados)
    resultado_duplicados = _enriquecer_dataset_com_status(df_usuarios_duplicados, df_status_ultimo)
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
