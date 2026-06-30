from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src.services.texto_service import normalizar_texto_serie
from src.utils.arquivos import salvar_dataframe


FUSO_BRASILIA = 'America/Sao_Paulo'
ARQUIVO_DISPARO_DIA = 'disparo_dia.csv'
ARQUIVO_DISPARO_DIA_SEGUINTE = 'disparo_dia_seguinte.csv'

COLUNAS_DISPARO_DIA = [
    'Telefone',
    'Nome',
    'Email',
    'cpf/cnpj',
    'id_Mailing',
    'nome_beneficiario',
    'flow_var_nomeBeneficiario',
    'flow_var_nomeCirurgia',
    'flow_var_nomeHospital',
    'flow_var_dataCirurgia',
]

COLUNAS_TELEFONE_USUARIOS = [
    'TELEFONE 1',
    'TELEFONE 2',
    'TELEFONE 3',
    'TELEFONE 4',
    'TELEFONE 5',
]


def _data_brasilia():
    return datetime.now(ZoneInfo(FUSO_BRASILIA)).date()


def _serie_texto(df, coluna):
    if coluna not in df.columns:
        return pd.Series('', index=df.index, dtype='object')
    return normalizar_texto_serie(df[coluna])


def _primeiro_valor_preenchido(df, colunas):
    saida = pd.Series('', index=df.index, dtype='object')
    for coluna in colunas:
        serie = _serie_texto(df, coluna)
        mask = (saida == '') & (serie != '')
        saida.loc[mask] = serie.loc[mask]
    return saida


def _serie_telefone_saida(df, coluna_telefone):
    if isinstance(coluna_telefone, (list, tuple)):
        return _primeiro_valor_preenchido(df, coluna_telefone)
    return _serie_texto(df, coluna_telefone)


def _serie_dia_mes(df, coluna):
    if coluna not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype='Int64')
    datas = pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)
    return datas.dt.day.astype('Int64')


def _serie_data(df, coluna):
    if coluna not in df.columns:
        return pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    return pd.to_datetime(df[coluna], errors='coerce', dayfirst=True)


def _montar_saida(df, coluna_telefone):
    saida = pd.DataFrame(index=df.index)
    saida['Telefone'] = _serie_telefone_saida(df, coluna_telefone)
    saida['Nome'] = _serie_texto(df, 'CHAVE RELATORIO')
    saida['Email'] = ''
    saida['cpf/cnpj'] = ''
    saida['id_Mailing'] = ''
    saida['nome_beneficiario'] = _serie_texto(df, 'USUARIO')
    saida['flow_var_nomeBeneficiario'] = ''
    saida['flow_var_nomeCirurgia'] = _serie_texto(df, 'PROCEDIMENTO')
    saida['flow_var_nomeHospital'] = _serie_texto(df, 'PRESTADOR')
    saida['flow_var_dataCirurgia'] = _serie_texto(df, 'DT INTERNACAO')
    return saida[COLUNAS_DISPARO_DIA].copy()


def _filtrar_usuarios_dia(df_usuarios, data_referencia):
    status_chave = _serie_texto(df_usuarios, 'STATUS CHAVE').str.upper()
    datas_internacao = _serie_data(df_usuarios, 'DT INTERNACAO')
    data_limite = pd.Timestamp(data_referencia) - pd.DateOffset(months=1)
    telefone_disponivel = _primeiro_valor_preenchido(df_usuarios, COLUNAS_TELEFONE_USUARIOS)
    mask = (
        (status_chave == 'SEM_MATCH')
        & datas_internacao.notna()
        & (datas_internacao <= data_limite)
        & (telefone_disponivel != '')
    )
    return df_usuarios[mask].copy()


def _filtrar_usuarios_dia_seguinte(df_usuarios, data_referencia):
    dia_seguinte = (data_referencia + timedelta(days=1)).day
    dias_internacao = _serie_dia_mes(df_usuarios, 'DT INTERNACAO')
    return df_usuarios[dias_internacao == dia_seguinte].copy()


def _filtrar_disparo_dia(df_disparo, data_referencia):
    validacao_final = _serie_texto(df_disparo, 'VALIDACAO FINAL').str.upper()
    processo = _serie_texto(df_disparo, 'PROCESSO').str.upper()
    mask_validacao_ok = validacao_final == 'OK'

    dt_envio = _serie_data(df_disparo, 'DT ENVIO')
    limite_segundo_envio = pd.Timestamp(data_referencia) - pd.Timedelta(hours=48)
    mask_segundo_envio = (
        mask_validacao_ok
        & (processo == 'SEGUNDO_ENVIO')
        & dt_envio.notna()
        & (dt_envio <= limite_segundo_envio)
    )

    mask_demais_processos = mask_validacao_ok & (processo != 'SEGUNDO_ENVIO')
    if data_referencia.weekday() != 0:
        dias_internacao = _serie_dia_mes(df_disparo, 'DT INTERNACAO')
        mask_demais_processos = (
            mask_demais_processos
            & dias_internacao.notna()
            & (dias_internacao != data_referencia.day)
        )

    mask = mask_segundo_envio | mask_demais_processos
    return df_disparo[mask].copy()


def montar_disparo_dia(df_usuarios, df_disparo, data_referencia=None):
    if data_referencia is None:
        data_referencia = _data_brasilia()

    usuarios_dia = _filtrar_usuarios_dia(df_usuarios, data_referencia)
    disparo_dia = _filtrar_disparo_dia(df_disparo, data_referencia)

    saidas = [
        _montar_saida(usuarios_dia, COLUNAS_TELEFONE_USUARIOS),
        _montar_saida(disparo_dia, 'TELEFONE DISPARO'),
    ]
    return pd.concat(saidas, ignore_index=True).reindex(columns=COLUNAS_DISPARO_DIA)


def montar_disparo_dia_seguinte(df_usuarios, data_referencia=None):
    if data_referencia is None:
        data_referencia = _data_brasilia()

    usuarios_dia_seguinte = _filtrar_usuarios_dia_seguinte(df_usuarios, data_referencia)
    return _montar_saida(usuarios_dia_seguinte, COLUNAS_TELEFONE_USUARIOS).reset_index(drop=True)


def gerar_arquivos_disparo_dia(
    arquivo_dataset_final,
    pasta_saida=None,
    data_referencia=None,
):
    if data_referencia is None:
        data_referencia = _data_brasilia()

    arquivo_dataset_final = Path(arquivo_dataset_final)
    if pasta_saida is None:
        pasta_saida = arquivo_dataset_final.parent
    pasta_saida = Path(pasta_saida)

    planilhas = pd.read_excel(arquivo_dataset_final, sheet_name=None, dtype=str).copy()
    df_usuarios = planilhas.get('usuarios', pd.DataFrame()).fillna('')
    df_disparo = planilhas.get('disparo', pd.DataFrame()).fillna('')

    df_disparo_dia = montar_disparo_dia(
        df_usuarios,
        df_disparo,
        data_referencia=data_referencia,
    )
    arquivo_disparo_dia = pasta_saida / ARQUIVO_DISPARO_DIA
    salvar_dataframe(df_disparo_dia, arquivo_disparo_dia)

    arquivos = {
        'disparo_dia': str(arquivo_disparo_dia),
    }
    totais = {
        'disparo_dia': len(df_disparo_dia),
    }

    if data_referencia.weekday() == 4:
        df_disparo_dia_seguinte = montar_disparo_dia_seguinte(
            df_usuarios,
            data_referencia=data_referencia,
        )
        arquivo_dia_seguinte = pasta_saida / ARQUIVO_DISPARO_DIA_SEGUINTE
        salvar_dataframe(df_disparo_dia_seguinte, arquivo_dia_seguinte)
        arquivos['disparo_dia_seguinte'] = str(arquivo_dia_seguinte)
        totais['disparo_dia_seguinte'] = len(df_disparo_dia_seguinte)

    return {
        'ok': True,
        'arquivos': arquivos,
        'totais': totais,
        'data_referencia': data_referencia.isoformat(),
        'dia_semana': data_referencia.weekday(),
        'mensagens': ['Arquivos de disparo do dia gerados com sucesso.'],
    }
