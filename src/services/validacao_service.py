import re
import pandas as pd
from src.config.schemas import (
    COLUNAS_OBRIGATORIAS_DATASET_ORIGEM,
    COLUNAS_STATUS_RESPOSTA_OBRIGATORIAS_PADRONIZACAO,
    COLUNAS_STATUS_OBRIGATORIAS_PADRONIZACAO,
)
from src.services.schema_resposta_service import (
    colunas_data_atendimento_presentes,
    diagnosticar_coluna_resposta,
    tem_coluna_data_atendimento,
    tem_coluna_resposta,
)


def validar_colunas_origem_para_padronizacao(
    df_status,
    df_status_resposta,
):
    resultado = {
        'ok': True,
        'mensagens': [],
    }

    colunas_status_obrigatorias = COLUNAS_STATUS_OBRIGATORIAS_PADRONIZACAO
    faltando_status = [c for c in colunas_status_obrigatorias if c not in df_status.columns]
    faltando_status_resposta = []
    for coluna in COLUNAS_STATUS_RESPOSTA_OBRIGATORIAS_PADRONIZACAO:
        if coluna == 'resposta':
            if not tem_coluna_resposta(df_status_resposta):
                faltando_status_resposta.append('resposta (ou alias legado Resposta/RESPOSTA)')
            continue
        if coluna not in df_status_resposta.columns:
            faltando_status_resposta.append(coluna)
    if not tem_coluna_data_atendimento(df_status_resposta):
        faltando_status_resposta.append('dat_atendimento ou DT_ATENDIMENTO')

    if faltando_status:
        resultado['ok'] = False
        resultado['mensagens'].append(
            f'Arquivo status.csv com estrutura alterada. Colunas faltando: {faltando_status}.'
        )

    if faltando_status_resposta:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Arquivo status_resposta com estrutura alterada. '
            f'Colunas faltando: {faltando_status_resposta}.'
        )

    if resultado['ok']:
        resultado['mensagens'].append(
            'Colunas de origem para padronizacao foram encontradas com sucesso.'
        )
        diagnostico_resposta = diagnosticar_coluna_resposta(df_status_resposta)
        aliases_presentes = diagnostico_resposta['aliases_presentes']
        qtd_linhas_conflito = diagnostico_resposta['qtd_linhas_conflito']

        colunas_data_presentes = colunas_data_atendimento_presentes(df_status_resposta)
        resultado['mensagens'].append(
            'Diagnostico data atendimento no status_resposta: '
            f'aliases_presentes={colunas_data_presentes}.'
        )

        resultado['mensagens'].append(
            'Diagnostico coluna resposta no status_resposta: '
            f'aliases_presentes={aliases_presentes}.'
        )
        if qtd_linhas_conflito > 0:
            resultado['mensagens'].append(
                'Aviso: conflito detectado entre aliases de resposta no status_resposta. '
                f'linhas_com_valores_distintos={qtd_linhas_conflito}.'
            )

    return resultado


def validar_padronizacao_colunas_data(df_status, df_status_resposta):
    resultado = {
        'ok': True,
        'mensagens': [],
    }

    if 'Data agendamento' not in df_status.columns:
        resultado['ok'] = False
        resultado['mensagens'].append('Coluna Data agendamento nao encontrada no arquivo status.')

    if 'Data agendamento' in df_status.columns and not pd.api.types.is_datetime64_any_dtype(
        df_status['Data agendamento']
    ):
        resultado['ok'] = False
        resultado['mensagens'].append('Coluna Data agendamento nao esta no tipo data.')

    if 'DT ENVIO' not in df_status.columns:
        resultado['ok'] = False
        resultado['mensagens'].append('Coluna DT ENVIO nao encontrada no arquivo status.')

    if 'DT_ATENDIMENTO' not in df_status_resposta.columns:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Coluna DT_ATENDIMENTO nao encontrada no arquivo status_resposta.'
        )

    if 'DT_ATENDIMENTO' in df_status_resposta.columns and not pd.api.types.is_datetime64_any_dtype(
        df_status_resposta['DT_ATENDIMENTO']
    ):
        resultado['ok'] = False
        resultado['mensagens'].append('Coluna DT_ATENDIMENTO nao esta no tipo data.')

    if resultado['ok']:
        resultado['mensagens'].append('Padronizacao de datas validada com sucesso.')

    return resultado


def validar_colunas_origem_dataset_complicacao(colunas_arquivo, contexto='dataset'):
    colunas_obrigatorias = COLUNAS_OBRIGATORIAS_DATASET_ORIGEM
    colunas_normalizadas = [str(c).strip() for c in colunas_arquivo]
    colunas_com_valor = [c for c in colunas_normalizadas if c != '']
    cabecalho_sem_valor_linha_1 = len(colunas_com_valor) == 0
    cabecalho_so_unnamed = (
        len(colunas_com_valor) > 0
        and all(c.lower().startswith('unnamed:') for c in colunas_com_valor)
    )

    if cabecalho_sem_valor_linha_1 or cabecalho_so_unnamed:
        return {
            'ok': False,
            'mensagens': [
                (
                    f'Cabecalho invalido no dataset de {contexto}: '
                    'na linha 1 do cabecalho nao ha dados validos.'
                ),
                (
                    'Nao foi encontrado nenhum valor de cabecalho na linha 1. '
                    'Revise a planilha e garanta que os nomes das colunas estejam na primeira linha.'
                ),
            ],
            'colunas_obrigatorias': colunas_obrigatorias,
            'colunas_faltando': colunas_obrigatorias,
            'colunas_duplicadas': [],
            'colunas_mascaradas_duplicadas': [],
        }

    set_colunas = set(colunas_normalizadas)
    faltando = [col for col in colunas_obrigatorias if col not in set_colunas]
    padrao_coluna_mascarada = re.compile(r'^P[1-4]\.\d+$')
    colunas_mascaradas_duplicadas = sorted(
        [col for col in colunas_normalizadas if padrao_coluna_mascarada.match(col)]
    )
    colunas_duplicadas = sorted(
        {
            col
            for col in colunas_obrigatorias
            if colunas_normalizadas.count(col) > 1
        }
    )

    if colunas_mascaradas_duplicadas:
        return {
            'ok': False,
            'mensagens': [
                (
                    f'Coluna essencial duplicada mascarada detectada no dataset de {contexto}: '
                    f'{colunas_mascaradas_duplicadas}'
                ),
                'Renomeie ou apague a coluna duplicada na origem antes de executar o pipeline.',
            ],
            'colunas_obrigatorias': colunas_obrigatorias,
            'colunas_faltando': faltando,
            'colunas_duplicadas': colunas_duplicadas,
            'colunas_mascaradas_duplicadas': colunas_mascaradas_duplicadas,
        }

    if colunas_duplicadas:
        return {
            'ok': False,
            'mensagens': [
                (
                    f'Colunas obrigatorias do dataset de {contexto} estao duplicadas. '
                    f'Duplicadas: {colunas_duplicadas}'
                ),
                'Renomeie ou apague a coluna duplicada para manter apenas uma ocorrencia de cada.',
            ],
            'colunas_obrigatorias': colunas_obrigatorias,
            'colunas_faltando': faltando,
            'colunas_duplicadas': colunas_duplicadas,
            'colunas_mascaradas_duplicadas': [],
        }

    if faltando:
        return {
            'ok': False,
            'mensagens': [
                f'Colunas obrigatorias do dataset de {contexto} nao foram encontradas.',
                f'Colunas faltando: {faltando}',
            ],
            'colunas_obrigatorias': colunas_obrigatorias,
            'colunas_faltando': faltando,
            'colunas_duplicadas': [],
            'colunas_mascaradas_duplicadas': [],
        }

    return {
        'ok': True,
        'mensagens': [f'Todas as colunas obrigatorias do dataset de {contexto} foram encontradas.'],
        'colunas_obrigatorias': colunas_obrigatorias,
        'colunas_faltando': [],
        'colunas_duplicadas': [],
        'colunas_mascaradas_duplicadas': [],
    }
