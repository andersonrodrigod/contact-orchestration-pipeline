from datetime import datetime
from pathlib import Path
import shutil
import re

import pandas as pd

from src.services.texto_service import simplificar_texto
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe

QT_COLUNAS = [
    'QT LIDA_RESPOSTA_SIM',
    'QT LIDA_RESPOSTA_NAO',
    'QT LIDA_SEM_RESPOSTA',
    'QT LIDA',
    'QT ENTREGUE',
    'QT ENVIADA',
    'QT NAO_ENTREGUE_META',
    'QT MENSAGEM_NAO_ENTREGUE',
    'QT EXPERIMENTO',
    'QT OPT_OUT',
]

SEM_QT_COLUNAS = [
    'LIDA_RESPOSTA_SIM',
    'LIDA_RESPOSTA_NAO',
    'LIDA_SEM_RESPOSTA',
    'LIDA',
    'ENTREGUE',
    'ENVIADA',
    'NAO_ENTREGUE_META',
    'MENSAGEM_NAO_ENTREGUE',
    'EXPERIMENTO',
    'OPT_OUT',
]
MESES_PT_BR = {
    '01': 'JANEIRO',
    '02': 'FEVEREIRO',
    '03': 'MARCO',
    '04': 'ABRIL',
    '05': 'MAIO',
    '06': 'JUNHO',
    '07': 'JULHO',
    '08': 'AGOSTO',
    '09': 'SETEMBRO',
    '10': 'OUTUBRO',
    '11': 'NOVEMBRO',
    '12': 'DEZEMBRO',
}
MESES_PT_BR_INV = {v: k for k, v in MESES_PT_BR.items()}


def _sanitizar_nome_processo(nome_processo):
    base = simplificar_texto(nome_processo).replace(' ', '_')
    if base == '':
        return 'execucao'
    return base


def _resolver_nome_execucao(nome_execucao=None, nome_processo='envio_status'):
    if nome_execucao:
        return str(nome_execucao).strip()
    prefixo = _sanitizar_nome_processo(nome_processo)
    return f"{prefixo}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"


def _nome_pasta_competencia(competencia):
    ano, mes = competencia.split('-')
    mes_nome = MESES_PT_BR.get(mes, mes)
    return f'STATUS_ENVIADO_{mes_nome}_{ano}'


def _backup_para_lixeira(pasta_destino, pasta_lixeira):
    if not pasta_destino.exists():
        return ''
    if not any(pasta_destino.iterdir()):
        return ''
    pasta_lixeira.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    destino_backup = pasta_lixeira / f'{pasta_destino.name}_{timestamp}'
    shutil.move(str(pasta_destino), str(destino_backup))
    return str(destino_backup)


def _serie_numerica(df, coluna):
    if coluna not in df.columns:
        return pd.Series(0, index=df.index, dtype='float64')
    return pd.to_numeric(df[coluna], errors='coerce').fillna(0)


def _serie_data(df, coluna):
    if coluna not in df.columns:
        return pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    serie_texto = df[coluna].astype(str).str.strip()
    mask_iso = serie_texto.str.match(r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$')
    serie_data = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
    if mask_iso.any():
        serie_data.loc[mask_iso] = pd.to_datetime(
            serie_texto.loc[mask_iso], errors='coerce', dayfirst=False
        )
    if (~mask_iso).any():
        serie_data.loc[~mask_iso] = pd.to_datetime(
            serie_texto.loc[~mask_iso], errors='coerce', dayfirst=True
        )
    return serie_data


def _filtrar_df_valido_temporal(df):
    dt_envio = _serie_data(df, 'DT ENVIO')
    dt_internacao = _serie_data(df, 'DT INTERNACAO')
    tem_ambas_datas = dt_envio.notna() & dt_internacao.notna()
    mes_ano_igual = (
        (dt_envio.dt.year == dt_internacao.dt.year)
        & (dt_envio.dt.month == dt_internacao.dt.month)
    )
    valido_temporal = ~tem_ambas_datas | ((dt_envio >= dt_internacao) & ~mes_ano_igual)
    return df.loc[valido_temporal].copy()


def _serie_competencia_internacao(df):
    serie_data = _serie_data(df, 'DT INTERNACAO')
    competencia = pd.Series(
        serie_data.dt.strftime('%Y-%m'),
        index=df.index,
        dtype='object',
    )
    if 'DT INTERNACAO' not in df.columns:
        return competencia.fillna('SEM_COMPETENCIA')

    faltantes = competencia.isna()
    if not faltantes.any():
        return competencia.fillna('SEM_COMPETENCIA')

    serie_texto = df['DT INTERNACAO'].astype(str).str.strip()

    def _normalizar_mes_ano(valor):
        txt = simplificar_texto(valor).upper()
        txt = re.sub(r'[^A-Z0-9]+', '_', txt).strip('_')
        return txt

    normalizada = serie_texto.loc[faltantes].map(_normalizar_mes_ano)
    extraido = normalizada.str.extract(r'^(?P<mes>[A-Z]+)_(?:DE_)?(?P<ano>\d{4})$')
    mes_num = extraido['mes'].map(MESES_PT_BR_INV)
    competencia_textual = extraido['ano'].where(mes_num.notna(), '') + '-' + mes_num.fillna('')
    competencia.loc[faltantes] = competencia_textual.where(
        competencia_textual.str.match(r'^\d{4}-\d{2}$'),
        pd.NA,
    )
    return competencia.fillna('SEM_COMPETENCIA')


def _to_int_if_whole(value):
    try:
        numero = float(value)
    except (TypeError, ValueError):
        return value
    if pd.isna(numero):
        return value
    if numero.is_integer():
        return int(numero)
    return numero


def _normalizar_df_numerico(df):
    df_out = df.copy()
    for coluna in df_out.columns:
        if pd.api.types.is_numeric_dtype(df_out[coluna]):
            df_out[coluna] = df_out[coluna].apply(_to_int_if_whole)
    return df_out


def _contagem_registros_validos(df, colunas):
    linhas = []
    for coluna in colunas:
        if coluna not in df.columns:
            linhas.append({'coluna': coluna, 'total_registros_validos': 0})
            continue
        serie = df[coluna]
        validos = int((serie.notna() & (serie.astype(str).str.strip() != '')).sum())
        linhas.append({'coluna': coluna, 'total_registros_validos': validos})
    return pd.DataFrame(linhas)


def _soma_por_coluna(df, colunas):
    linhas = []
    for coluna in colunas:
        total = _to_int_if_whole(_serie_numerica(df, coluna).sum()) if coluna in df.columns else 0
        linhas.append({'coluna': coluna, 'soma_total': total})
    return pd.DataFrame(linhas)


def _soma_por_linha(df, colunas, excluir):
    usar = [c for c in colunas if c not in set(excluir)]
    soma = pd.Series(0, index=df.index, dtype='float64')
    for coluna in usar:
        soma = soma + _serie_numerica(df, coluna)
    return soma


def _gerar_bloco_colunas(df, pasta_saida, colunas, tipo, coluna_lida, colunas_resposta_lida):
    soma_linha_exceto_lida = _soma_por_linha(df, colunas, excluir=[coluna_lida])
    soma_linha_exceto_resposta = _soma_por_linha(df, colunas, excluir=colunas_resposta_lida)
    _ = soma_linha_exceto_lida
    _ = soma_linha_exceto_resposta

    soma_col_exceto_lida = _soma_por_coluna(
        df,
        [c for c in colunas if c != coluna_lida],
    )
    nome_soma_col_lida = (
        'QT_SOMA_COLUNA_SEM_LIDA.csv'
        if tipo == 'com_qt'
        else 'SEM_QT_SOMA_COLUNA_SEM_LIDA.csv'
    )
    salvar_dataframe(_normalizar_df_numerico(soma_col_exceto_lida), pasta_saida / nome_soma_col_lida)

    soma_col_exceto_resposta = _soma_por_coluna(
        df,
        [c for c in colunas if c not in set(colunas_resposta_lida)],
    )
    nome_soma_col_resp = (
        'QT_SOMA_COLUNA_SEM_RESPOSTA_LIDA.csv'
        if tipo == 'com_qt'
        else 'SEM_QT_SOMA_COLUNA_SEM_RESPOSTA_LIDA.csv'
    )
    salvar_dataframe(_normalizar_df_numerico(soma_col_exceto_resposta), pasta_saida / nome_soma_col_resp)

    contagem_validos = _contagem_registros_validos(df, colunas)
    nome_validos = 'QT_VALIDOS.csv' if tipo == 'com_qt' else 'SEM_QT_VALIDOS.csv'
    salvar_dataframe(_normalizar_df_numerico(contagem_validos), pasta_saida / nome_validos)


def _gerar_metricas_qt_telefones(df, pasta_saida):
    coluna = 'QT TELEFONES'
    serie_numerica = _serie_numerica(df, coluna) if coluna in df.columns else pd.Series([], dtype='float64')
    soma_total = _to_int_if_whole(serie_numerica.sum()) if len(serie_numerica) > 0 else 0
    salvar_dataframe(
        pd.DataFrame([{'metrica': 'qt_telefones_soma_total', 'valor': soma_total}]),
        pasta_saida / 'QT_TELEFONES.csv',
    )

    if coluna not in df.columns:
        salvar_dataframe(
            pd.DataFrame(columns=['valor_qt_telefones', 'ocorrencias']),
            pasta_saida / 'QT_TELEFONES_OCORRENCIAS.csv',
        )
        return

    serie_original = df[coluna]
    mask_preenchido = serie_original.notna() & (serie_original.astype(str).str.strip() != '')
    serie_valor = serie_original.loc[mask_preenchido].astype(str).str.strip()

    if len(serie_valor) == 0:
        salvar_dataframe(
            pd.DataFrame(columns=['valor_qt_telefones', 'ocorrencias']),
            pasta_saida / 'QT_TELEFONES_OCORRENCIAS.csv',
        )
        return

    valores_num = pd.to_numeric(serie_valor, errors='coerce')
    serie_valor.loc[valores_num.notna()] = valores_num[valores_num.notna()].apply(
        lambda v: str(int(v)) if float(v).is_integer() else str(v)
    )
    ocorrencias = (
        serie_valor.value_counts(dropna=False)
        .rename_axis('valor_qt_telefones')
        .reset_index(name='ocorrencias')
        .sort_values(['ocorrencias', 'valor_qt_telefones'], ascending=[False, True])
    )
    salvar_dataframe(ocorrencias, pasta_saida / 'QT_TELEFONES_OCORRENCIAS.csv')


def gerar_analise_dados_fase2_csv(
    arquivo_dataset_status,
    raiz_analise='src/data/analise_dados',
    nome_execucao=None,
    nome_processo='envio_status',
):
    _ = nome_execucao
    _ = nome_processo
    raiz_status_enviado = Path(raiz_analise) / 'status_enviado'
    raiz_lixeira = raiz_status_enviado / 'lixeira'
    raiz_status_enviado.mkdir(parents=True, exist_ok=True)

    df = ler_arquivo_csv(arquivo_dataset_status)
    df = _filtrar_df_valido_temporal(df)
    serie_competencia = _serie_competencia_internacao(df)
    mensagens = []
    if 'DT INTERNACAO' in df.columns:
        serie_bruta = df['DT INTERNACAO'].astype(str).str.strip()
        mask_texto = serie_bruta != ''
        mask_invalida = (serie_competencia == 'SEM_COMPETENCIA') & mask_texto
        if mask_invalida.any():
            exemplos_invalidos = sorted(serie_bruta.loc[mask_invalida].dropna().unique().tolist())[:5]
            exemplos_txt = ', '.join(exemplos_invalidos)
            mensagens.append(
                'DT INTERNACAO com formato nao reconhecido em parte dos registros. '
                'Use data valida (ex.: 10/03/2026) ou MES_ANO (ex.: DEZEMBRO_2026 ou DEZEMBRO_DE_2026). '
                f'Exemplos invalidos encontrados: {exemplos_txt}'
            )
    competencias = serie_competencia.unique().tolist()
    competencias = sorted([c for c in competencias if c != 'SEM_COMPETENCIA'])

    if not competencias:
        return {
            'ok': True,
            'pasta_saida': str(raiz_status_enviado),
            'pastas_saida': [],
            'pastas_lixeira': [],
            'arquivos_gerados': [],
            'mensagem': 'Nenhuma competencia valida de DT INTERNACAO encontrada para envio de status.',
            'mensagens': mensagens,
        }

    pastas_saida = []
    backups = []
    arquivos_gerados = []

    for competencia in competencias:
        mask_comp = (
            serie_competencia == competencia
        )
        df_comp = df.loc[mask_comp].copy().reset_index(drop=True)
        if len(df_comp) == 0:
            continue

        nome_pasta = _nome_pasta_competencia(competencia)
        pasta_saida = raiz_status_enviado / nome_pasta
        backup = _backup_para_lixeira(pasta_saida, raiz_lixeira)
        if backup:
            backups.append(backup)
        pasta_saida.mkdir(parents=True, exist_ok=True)

        _gerar_bloco_colunas(
            df=df_comp,
            pasta_saida=pasta_saida,
            colunas=QT_COLUNAS,
            tipo='com_qt',
            coluna_lida='QT LIDA',
            colunas_resposta_lida=[
                'QT LIDA_RESPOSTA_SIM',
                'QT LIDA_RESPOSTA_NAO',
                'QT LIDA_SEM_RESPOSTA',
            ],
        )
        _gerar_bloco_colunas(
            df=df_comp,
            pasta_saida=pasta_saida,
            colunas=SEM_QT_COLUNAS,
            tipo='sem_qt',
            coluna_lida='LIDA',
            colunas_resposta_lida=[
                'LIDA_RESPOSTA_SIM',
                'LIDA_RESPOSTA_NAO',
                'LIDA_SEM_RESPOSTA',
            ],
        )
        _gerar_metricas_qt_telefones(df_comp, pasta_saida)

        pastas_saida.append(str(pasta_saida))
        arquivos_gerados.extend(
            [
                str(pasta_saida / 'QT_SOMA_COLUNA_SEM_LIDA.csv'),
                str(pasta_saida / 'QT_SOMA_COLUNA_SEM_RESPOSTA_LIDA.csv'),
                str(pasta_saida / 'SEM_QT_SOMA_COLUNA_SEM_LIDA.csv'),
                str(pasta_saida / 'SEM_QT_SOMA_COLUNA_SEM_RESPOSTA_LIDA.csv'),
                str(pasta_saida / 'QT_VALIDOS.csv'),
                str(pasta_saida / 'SEM_QT_VALIDOS.csv'),
                str(pasta_saida / 'QT_TELEFONES.csv'),
                str(pasta_saida / 'QT_TELEFONES_OCORRENCIAS.csv'),
            ]
        )

    return {
        'ok': True,
        'nome_execucao': 'status_enviado',
        'pasta_saida': pastas_saida[0] if pastas_saida else str(raiz_status_enviado),
        'pastas_saida': pastas_saida,
        'pastas_lixeira': backups,
        'arquivos_gerados': arquivos_gerados,
        'mensagens': mensagens,
    }
