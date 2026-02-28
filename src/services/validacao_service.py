import pandas as pd


def _tem_coluna_data_atendimento(df):
    return 'dat_atendimento' in df.columns or 'DT_ATENDIMENTO' in df.columns


def validar_colunas_origem_para_padronizacao(df_status, df_status_resposta):
    resultado = {
        'ok': True,
        'mensagens': [],
    }

    colunas_status_obrigatorias = [
        'Data agendamento',
        'HSM',
        'Status',
        'Respondido',
        'Contato',
        'Telefone',
    ]
    colunas_status_resposta_obrigatorias = ['resposta', 'nom_contato']

    faltando_status = [c for c in colunas_status_obrigatorias if c not in df_status.columns]
    faltando_status_resposta = [c for c in colunas_status_resposta_obrigatorias if c not in df_status_resposta.columns]
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


def validar_colunas_identicas(df_eletivo, df_internacao):
    colunas_eletivo = list(df_eletivo.columns)
    colunas_internacao = list(df_internacao.columns)

    set_eletivo = set(colunas_eletivo)
    set_internacao = set(colunas_internacao)

    if set_eletivo == set_internacao:
        return {
            'ok': True,
            'mensagens': ['Arquivos de resposta possuem colunas identicas para concatenacao.'],
        }

    faltando_no_internacao = sorted(set_eletivo - set_internacao)
    faltando_no_eletivo = sorted(set_internacao - set_eletivo)

    mensagens = ['Arquivos de resposta NAO possuem colunas identicas. Concatenacao ignorada.']
    if faltando_no_internacao:
        mensagens.append(
            f'Colunas presentes no eletivo e ausentes no internacao: {faltando_no_internacao}'
        )
    if faltando_no_eletivo:
        mensagens.append(
            f'Colunas presentes no internacao e ausentes no eletivo: {faltando_no_eletivo}'
        )

    return {'ok': False, 'mensagens': mensagens}


def validar_colunas_minimas_status_resposta(df_eletivo, df_internacao):
    colunas_minimas = {'resposta', 'nom_contato'}

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
