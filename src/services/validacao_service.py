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
    colunas_status_resposta_obrigatorias = [
        'dat_atendimento',
        'resposta',
        'nom_contato',
    ]

    faltando_status = [c for c in colunas_status_obrigatorias if c not in df_status.columns]
    faltando_status_resposta = [
        c for c in colunas_status_resposta_obrigatorias if c not in df_status_resposta.columns
    ]

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

    if 'DT_ENVIO' not in df_status.columns:
        resultado['ok'] = False
        resultado['mensagens'].append('Coluna DT_ENVIO nao encontrada no arquivo status.')
    else:
        qtd_nulos = int(df_status['DT_ENVIO'].isna().sum())
        if qtd_nulos > 0:
            resultado['ok'] = False
            resultado['mensagens'].append(
                f'Coluna DT_ENVIO possui {qtd_nulos} valores invalidos ou vazios.'
            )

    if 'DT_ATENDIMENTO' not in df_status_resposta.columns:
        resultado['ok'] = False
        resultado['mensagens'].append(
            'Coluna DT_ATENDIMENTO nao encontrada no arquivo status_resposta.'
        )
    else:
        qtd_nulos = int(df_status_resposta['DT_ATENDIMENTO'].isna().sum())
        if qtd_nulos > 0:
            resultado['ok'] = False
            resultado['mensagens'].append(
                f'Coluna DT_ATENDIMENTO possui {qtd_nulos} valores invalidos ou vazios.'
            )

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
