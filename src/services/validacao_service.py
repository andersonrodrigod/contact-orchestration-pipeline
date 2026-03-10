import re
import pandas as pd
from src.config.governanca_config import (
    resolver_janela_corte_alias_resposta,
    resolver_modo_estrito_data_atendimento,
    resolver_modo_estrito_alias_resposta,
)
from src.config.schemas import (
    COLUNAS_MINIMAS_STATUS_CONCATENACAO,
    COLUNAS_MINIMAS_STATUS_RESPOSTA_CONCATENACAO,
    COLUNAS_OBRIGATORIAS_DATASET_ORIGEM,
    COLUNAS_STATUS_RESPOSTA_OBRIGATORIAS_PADRONIZACAO,
    COLUNAS_STATUS_OBRIGATORIAS_PADRONIZACAO,
)
from src.services.schema_resposta_service import (
    aliases_data_atendimento_legado_presentes,
    aliases_resposta_legado_presentes,
    colunas_data_atendimento_presentes,
    diagnosticar_coluna_resposta,
    tem_coluna_data_atendimento,
    tem_coluna_resposta,
)


def _tem_coluna_data_atendimento(df):
    return tem_coluna_data_atendimento(df)


def _tem_coluna_resposta(df):
    return tem_coluna_resposta(df)


def validar_colunas_origem_para_padronizacao(
    df_status,
    df_status_resposta,
    modo_estrito_alias_resposta=None,
    janela_corte_alias_resposta_ciclos=None,
    modo_estrito_data_atendimento=None,
):
    modo_estrito_alias_resposta, _ = resolver_modo_estrito_alias_resposta(
        modo_estrito_alias_resposta
    )
    janela_corte_alias_resposta_ciclos, _ = resolver_janela_corte_alias_resposta(
        janela_corte_alias_resposta_ciclos
    )
    modo_estrito_data_atendimento, _ = resolver_modo_estrito_data_atendimento(
        modo_estrito_data_atendimento
    )
    resultado = {
        'ok': True,
        'mensagens': [],
        'warnings_alias_resposta_legado': 0,
        'aliases_legados_presentes': [],
        'modo_estrito_alias_resposta': modo_estrito_alias_resposta,
        'modo_estrito_data_atendimento': modo_estrito_data_atendimento,
        'janela_corte_alias_resposta_ciclos': janela_corte_alias_resposta_ciclos,
    }

    colunas_status_obrigatorias = COLUNAS_STATUS_OBRIGATORIAS_PADRONIZACAO
    faltando_status = [c for c in colunas_status_obrigatorias if c not in df_status.columns]
    faltando_status_resposta = []
    for coluna in COLUNAS_STATUS_RESPOSTA_OBRIGATORIAS_PADRONIZACAO:
        if coluna == 'resposta':
            if not _tem_coluna_resposta(df_status_resposta):
                faltando_status_resposta.append('resposta (ou alias legado Resposta/RESPOSTA)')
            continue
        if coluna not in df_status_resposta.columns:
            faltando_status_resposta.append(coluna)
    if not _tem_coluna_data_atendimento(df_status_resposta):
        faltando_status_resposta.append('dat_atendimento ou DT_ATENDIMENTO')

    if faltando_status:
        resultado['ok'] = False
        resultado['mensagens'].append(
            f'Arquivo status.csv com estrutura alterada. Colunas faltando: {faltando_status}. '
            'O codigo do app precisa ser adaptado para a nova alteracao.'
        )

    if faltando_status_resposta:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Arquivo status_resposta com estrutura alterada. '
            f'Colunas faltando: {faltando_status_resposta}. '
            'O codigo do app precisa ser adaptado para a nova alteracao.'
        )

    if resultado['ok']:
        resultado['mensagens'].append(
            'Colunas de origem para padronizacao foram encontradas com sucesso.'
        )
        diagnostico_resposta = diagnosticar_coluna_resposta(df_status_resposta)
        aliases_presentes = diagnostico_resposta['aliases_presentes']
        qtd_linhas_conflito = diagnostico_resposta['qtd_linhas_conflito']
        aliases_legados = aliases_resposta_legado_presentes(df_status_resposta)
        aliases_data_legado = aliases_data_atendimento_legado_presentes(df_status_resposta)
        resultado['aliases_legados_presentes'] = aliases_legados

        colunas_data_presentes = colunas_data_atendimento_presentes(df_status_resposta)
        resultado['mensagens'].append(
            'Diagnostico data atendimento no status_resposta: '
            f'aliases_presentes={colunas_data_presentes}.'
        )
        if len(aliases_data_legado) > 0:
            resultado['mensagens'].append(
                'Aviso de deprecacao: alias legado de data de atendimento detectado no status_resposta. '
                f'aliases_legados={aliases_data_legado}.'
            )
            if modo_estrito_data_atendimento:
                resultado['ok'] = False
                resultado['mensagens'].append(
                    'Modo estrito de data de atendimento habilitado: execucao bloqueada devido a alias legado.'
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
        if len(aliases_legados) > 0:
            resultado['warnings_alias_resposta_legado'] = 1
            resultado['mensagens'].append(
                'Aviso de deprecacao: aliases legados de resposta detectados no status_resposta. '
                f'aliases_legados={aliases_legados}. '
                f'Criterio de corte: 0 warning por {janela_corte_alias_resposta_ciclos} ciclos consecutivos.'
            )
            if modo_estrito_alias_resposta:
                resultado['ok'] = False
                resultado['mensagens'].append(
                    'Modo estrito de alias de resposta habilitado: execucao bloqueada devido a alias legado.'
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


def validar_colunas_minimas_status_resposta(df_eletivo, df_internacao):
    colunas_minimas = COLUNAS_MINIMAS_STATUS_RESPOSTA_CONCATENACAO

    faltando_eletivo = sorted(colunas_minimas - set(df_eletivo.columns))
    faltando_internacao = sorted(colunas_minimas - set(df_internacao.columns))
    if not _tem_coluna_data_atendimento(df_eletivo):
        faltando_eletivo.append('dat_atendimento ou DT_ATENDIMENTO')
    if not _tem_coluna_data_atendimento(df_internacao):
        faltando_internacao.append('dat_atendimento ou DT_ATENDIMENTO')

    if not faltando_eletivo and not faltando_internacao:
        return {
            'ok': True,
            'mensagens': ['Colunas minimas obrigatorias encontradas para concatenacao.'],
        }

    mensagens = ['Concatenacao interrompida: colunas minimas obrigatorias nao encontradas.']
    if faltando_eletivo:
        mensagens.append(f'Faltando no arquivo eletivo: {faltando_eletivo}')
    if faltando_internacao:
        mensagens.append(f'Faltando no arquivo internacao: {faltando_internacao}')

    return {'ok': False, 'mensagens': mensagens}


def validar_colunas_minimas_status_concatenacao(df_complicacao, df_internacao_eletivo):
    colunas_minimas = COLUNAS_MINIMAS_STATUS_CONCATENACAO

    faltando_complicacao = sorted(colunas_minimas - set(df_complicacao.columns))
    faltando_internacao_eletivo = sorted(colunas_minimas - set(df_internacao_eletivo.columns))

    if not faltando_complicacao and not faltando_internacao_eletivo:
        return {
            'ok': True,
            'mensagens': ['Colunas minimas obrigatorias encontradas para concatenacao de status.'],
        }

    mensagens = ['Concatenacao de status interrompida: colunas minimas obrigatorias nao encontradas.']
    if faltando_complicacao:
        mensagens.append(f'Faltando no arquivo status_complicacao: {faltando_complicacao}')
    if faltando_internacao_eletivo:
        mensagens.append(
            f'Faltando no arquivo status_internacao_eletivo: {faltando_internacao_eletivo}'
        )

    return {'ok': False, 'mensagens': mensagens}


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
