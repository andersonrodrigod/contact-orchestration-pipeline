import re
import unicodedata

import pandas as pd


def _chave_canonica_texto(texto):
    valor = str(texto).strip().lower()
    valor = unicodedata.normalize('NFKD', valor)
    valor = ''.join(ch for ch in valor if not unicodedata.combining(ch))
    # Mantem separacao sem inventar palavra/conector.
    valor = valor.replace('\ufffd', ' ')
    valor = re.sub(r'[^a-z0-9\s]+', ' ', valor)
    valor = re.sub(r'\s+', ' ', valor).strip()
    return valor


def _normalizar_fragmentos_quebrados_chave(chave):
    # Reconstrucao de palavras quebradas por encoding ruim.
    regras = [
        (r'\bn\s*mero\b', 'numero'),
        (r'\busu\s*rio\b', 'usuario'),
        (r'\bn\s*o\b', 'nao'),
        (r'\bcomplica\s*es\b', 'complicacoes'),
    ]
    saida = chave
    for padrao, repl in regras:
        saida = re.sub(padrao, repl, saida)
    saida = re.sub(r'\s+', ' ', saida).strip()
    return saida


def normalizar_chave_texto(valor):
    if pd.isna(valor):
        return ''
    texto = str(valor).strip()
    if texto == '':
        return ''
    texto = corrigir_texto_bugado(texto)
    chave = _chave_canonica_texto(texto)
    chave = _normalizar_fragmentos_quebrados_chave(chave)
    return chave


def _normalizar_frases_canonicas(texto):
    chave = _chave_canonica_texto(texto)
    chave = _normalizar_fragmentos_quebrados_chave(chave)

    canonicos = {
        'pesquisa complicacoes cirurgicas': 'Pesquisa Complicações Cirurgicas',
        'numero e parte de um experimento': 'Número é parte de um experimento',
        'usuario decidiu nao receber mkt messages': 'Usuário decidiu não receber MKT messages',
        'nao': 'Não',
    }
    if chave in canonicos:
        return canonicos[chave]

    # Fallback controlado (apenas quando houver contexto forte da frase).
    if 'pesquisa' in chave and 'complicacoes' in chave and 'cirurgicas' in chave:
        return 'Pesquisa Complicações Cirurgicas'
    if 'numero' in chave and 'parte de um experimento' in chave:
        return 'Número é parte de um experimento'
    if 'usuario decidiu' in chave and 'receber mkt messages' in chave and 'nao' in chave:
        return 'Usuário decidiu não receber MKT messages'

    return texto


def _tentar_redecodificar_mojibake(texto):
    marcadores = ['Ã', 'Â', 'â', '\ufffd']
    if not any(m in texto for m in marcadores):
        return texto

    tentativas = [
        ('latin1', 'utf-8'),
        ('cp1252', 'utf-8'),
    ]
    for origem, destino in tentativas:
        try:
            convertido = texto.encode(origem).decode(destino)
            if convertido:
                return convertido
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return texto


def corrigir_texto_bugado(texto):
    if pd.isna(texto):
        return texto

    texto = str(texto)

    trocas = {
        'Voc\u03a9s': 'Voc\u00eas',
        'N\u03c0o': 'N\u00e3o',
        'N\ufffdo': 'Não',
        'n\u03c0o': 'n\u00e3o',
        'n\ufffdo': 'não',
        'n\u00cf\u20aco': 'não',
        'Pesquisa Complica├º├╡es Cirurgicas': 'Pesquisa Complicações Cirurgicas',
        'Complica\u00cf\u201e\u00e2\u0152\u00a1es': 'Complicações',
        'N\u00c2\u00b7mero \u00ce\u02dc': 'Número é',
        'N\u00c2\u00b7mero': 'Número',
        'Usu\u00c3\u0178rio': 'Usuário',
        'Mensagem n\u00cf\u20aco': 'Mensagem não',
        '\u00cf\u20ac': '\u00e3',
        '\u00ce\u02dc': '\u00e9',
        '\u00c3\u00a7': '\u00e7',
        '\u00c3\u00a3': '\u00e3',
        '\u00c3\u00a1': '\u00e1',
        '\u00c3\u00a9': '\u00e9',
        '\u00c3\u00aa': '\u00ea',
        '\u00c3\u00b3': '\u00f3',
        '\u00c3\u00ba': '\u00fa',
        '\u00c3\u00b5': '\u00f5',
        '\u2013': '-',
        '\u2014': '-',
        '\u2212': '-',
    }

    for antigo, novo in trocas.items():
        texto = texto.replace(antigo, novo)

    texto = _tentar_redecodificar_mojibake(texto)
    texto = _normalizar_frases_canonicas(texto)

    return texto


def normalizar_tipos_dataframe(df, colunas_data=None):
    if colunas_data is None:
        colunas_data = []

    for coluna in df.columns:
        if coluna not in colunas_data:
            df[coluna] = df[coluna].astype(str)

    for coluna_data in colunas_data:
        if coluna_data in df.columns:
            serie_original = df[coluna_data].astype(str).str.strip()
            mask_valor = df[coluna_data].notna() & (serie_original != '')
            mask_iso = mask_valor & serie_original.str.match(
                r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$'
            )

            serie_data = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')

            # Parse dedicado de formatos ISO para evitar warning com dayfirst=True.
            if mask_iso.any():
                serie_data.loc[mask_iso] = pd.to_datetime(
                    df.loc[mask_iso, coluna_data],
                    errors='coerce',
                    dayfirst=False,
                )

            mask_restante = mask_valor & ~mask_iso
            if mask_restante.any():
                serie_data.loc[mask_restante] = pd.to_datetime(
                    df.loc[mask_restante, coluna_data],
                    errors='coerce',
                    dayfirst=True,
                )

            mask_nat_com_valor = serie_data.isna() & mask_valor
            if mask_nat_com_valor.any():
                # Fallback final para formatos alternativos sem depender de format='mixed'.
                serie_data.loc[mask_nat_com_valor] = pd.to_datetime(
                    df.loc[mask_nat_com_valor, coluna_data],
                    errors='coerce',
                    dayfirst=False,
                )
            df[coluna_data] = serie_data

    return df


def limpar_texto_exceto_colunas(df, colunas_ignorar=None):
    if colunas_ignorar is None:
        colunas_ignorar = []

    for coluna in df.columns:
        if coluna in colunas_ignorar:
            continue
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].apply(corrigir_texto_bugado)

    return df


def formatar_coluna_data_br(df, coluna):
    if coluna in df.columns:
        df[coluna] = df[coluna].dt.strftime('%d/%m/%Y')

    return df


def criar_coluna_dt_envio_por_data_agendamento(
    df,
    coluna_origem='Data agendamento',
    coluna_destino='DT ENVIO',
):
    if coluna_origem in df.columns:
        serie_data = pd.to_datetime(df[coluna_origem], errors='coerce', dayfirst=True)
        df[coluna_destino] = serie_data.dt.strftime('%d/%m/%Y')

    return df


def normalizar_telefone(valor):
    if pd.isna(valor):
        return ''

    texto = str(valor).strip()
    if texto.endswith('.0'):
        texto = texto[:-2]

    return re.sub(r'\D', '', texto)


def normalizar_colunas_telefone_dataframe(df, colunas_telefone=None):
    if colunas_telefone is None:
        colunas_telefone = []

    for coluna in colunas_telefone:
        if coluna in df.columns:
            df[coluna] = df[coluna].apply(normalizar_telefone)

    return df
