import hashlib
import json
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.services.texto_service import normalizar_texto_serie
from src.utils.arquivos import salvar_dataframe


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


def _normalizar_dt_envio(df):
    if 'DT ENVIO' not in df.columns:
        return pd.Series('SEM_DATA', index=df.index, dtype='object')
    serie_dt = _serie_data(df, 'DT ENVIO')
    return serie_dt.dt.strftime('%Y-%m-%d').fillna('SEM_DATA')


def _serie_data_internacao(df):
    serie_data = _serie_data(df, 'DT INTERNACAO')
    if 'DT INTERNACAO' not in df.columns:
        return serie_data

    faltantes = serie_data.isna()
    if not faltantes.any():
        return serie_data

    serie_texto = (
        df['DT INTERNACAO']
        .astype(str)
        .str.strip()
        .str.upper()
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('ascii')
        .map(lambda v: re.sub(r'[^A-Z0-9]+', '_', v).strip('_'))
    )
    extraido = serie_texto.loc[faltantes].str.extract(r'^(?P<mes>[A-Z]+)_(?:DE_)?(?P<ano>\d{4})$')
    mes_num = extraido['mes'].map(MESES_PT_BR_INV)
    data_texto = extraido['ano'].where(mes_num.notna(), '') + '-' + mes_num.fillna('') + '-01'
    data_mes = pd.to_datetime(data_texto, errors='coerce')
    serie_data.loc[faltantes] = data_mes
    return serie_data


def _serie_competencia_internacao(df):
    return _serie_data_internacao(df).dt.strftime('%Y-%m').fillna('SEM_COMPETENCIA')


def _normalizar_processo(df):
    if 'PROCESSO' not in df.columns:
        return pd.Series('', index=df.index, dtype='object')
    return normalizar_texto_serie(df['PROCESSO'])


def _normalizar_status_chave(df):
    if 'STATUS CHAVE' in df.columns:
        serie = normalizar_texto_serie(df['STATUS CHAVE'])
    elif 'STATUS_CHAVE' in df.columns:
        serie = normalizar_texto_serie(df['STATUS_CHAVE'])
    else:
        serie = pd.Series('', index=df.index, dtype='object')
    return serie.replace({'': 'VAZIO'})


def _classificar_acao(df):
    if 'ACAO' not in df.columns:
        return pd.Series('PROGRAMADO', index=df.index, dtype='object')

    serie = normalizar_texto_serie(df['ACAO'])
    chave = (
        serie.str.lower()
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('ascii')
        .str.replace(r'\s+', ' ', regex=True)
        .str.strip()
    )
    classificacao = pd.Series('EM_ANDAMENTO', index=df.index, dtype='object')
    classificacao.loc[chave == ''] = 'PROGRAMADO'
    classificacao.loc[chave == 'encerrado'] = 'ENCERRADO'
    classificacao.loc[chave == 'sem telefone'] = 'SEM TELEFONE'
    classificacao.loc[chave == 'sem telefone disponivel'] = 'SEM_TELEFONE_DISPONIVEL'
    return classificacao


def _aplicar_regra_temporal(df, serie_base, valor_quando_invalido='PROGRAMADO'):
    dt_envio = _serie_data(df, 'DT ENVIO')
    dt_internacao = _serie_data_internacao(df)

    tem_ambas_datas = dt_envio.notna() & dt_internacao.notna()
    mes_ano_igual = (
        (dt_envio.dt.year == dt_internacao.dt.year)
        & (dt_envio.dt.month == dt_internacao.dt.month)
    )
    # Invalida quando DT_ENVIO < DT_INTERNACAO ou mesmo mes/ano.
    valido_temporal = ~tem_ambas_datas | ((dt_envio >= dt_internacao) & ~mes_ano_igual)
    invalido_temporal = ~valido_temporal

    serie_ajustada = serie_base.copy()
    serie_ajustada.loc[invalido_temporal] = valor_quando_invalido
    return serie_ajustada, valido_temporal


def _contagem_total(serie, nome_chave):
    return (
        serie.value_counts(dropna=False)
        .rename_axis(nome_chave)
        .reset_index(name='TOTAL')
        .sort_values(['TOTAL', nome_chave], ascending=[False, True])
    )


def _contagem_por_data(dt_envio, serie, nome_chave):
    return (
        pd.DataFrame({'DATA': dt_envio, nome_chave: serie})
        .groupby(['DATA', nome_chave], dropna=False)
        .size()
        .reset_index(name='TOTAL')
        .sort_values(['DATA', 'TOTAL', nome_chave], ascending=[True, False, True])
    )


def _remover_chave_vazia(df, coluna_chave):
    if coluna_chave not in df.columns:
        return df
    return df[df[coluna_chave].astype(str).str.strip() != ''].copy()


def _somar_programado_no_total(df_processo_total, incremento_programado):
    if incremento_programado <= 0:
        return df_processo_total
    df_out = df_processo_total.copy()
    mask_prog = df_out['PROCESSO'] == 'PROGRAMADO'
    if mask_prog.any():
        df_out.loc[mask_prog, 'TOTAL'] = (
            pd.to_numeric(df_out.loc[mask_prog, 'TOTAL'], errors='coerce').fillna(0).astype(int)
            + int(incremento_programado)
        )
    else:
        df_out = pd.concat(
            [
                df_out,
                pd.DataFrame([{'PROCESSO': 'PROGRAMADO', 'TOTAL': int(incremento_programado)}]),
            ],
            ignore_index=True,
        )
    return df_out.sort_values(['TOTAL', 'PROCESSO'], ascending=[False, True]).reset_index(drop=True)


def _somar_programado_por_data(df_processo_data, df_programado_por_data):
    if len(df_programado_por_data) == 0:
        return df_processo_data
    df_out = df_processo_data.copy()
    for _, linha in df_programado_por_data.iterrows():
        data = linha['DATA']
        total_add = int(linha['TOTAL'])
        mask = (df_out['DATA'] == data) & (df_out['PROCESSO'] == 'PROGRAMADO')
        if mask.any():
            df_out.loc[mask, 'TOTAL'] = (
                pd.to_numeric(df_out.loc[mask, 'TOTAL'], errors='coerce').fillna(0).astype(int)
                + total_add
            )
        else:
            df_out = pd.concat(
                [
                    df_out,
                    pd.DataFrame([{'DATA': data, 'PROCESSO': 'PROGRAMADO', 'TOTAL': total_add}]),
                ],
                ignore_index=True,
            )
    return df_out.sort_values(['DATA', 'TOTAL', 'PROCESSO'], ascending=[True, False, True]).reset_index(drop=True)


def _carregar_aba_planilha(arquivo_dataset_orquestrado, nome_aba):
    try:
        return pd.read_excel(
            arquivo_dataset_orquestrado,
            sheet_name=nome_aba,
            dtype=str,
            keep_default_na=False,
        )
    except ValueError:
        return pd.DataFrame()


def _nome_pasta_competencia(competencia):
    ano, mes = competencia.split('-')
    mes_nome = MESES_PT_BR.get(mes, mes)
    return f'ORQUESTRACAO_{mes_nome}_{ano}'


def _arquivo_json_competencia(competencia):
    ano, mes = competencia.split('-')
    return f'orquestracao_metricas_{ano}_{mes}.json'


def _hash_payload(payload_dia):
    conteudo = json.dumps(payload_dia, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(conteudo.encode('utf-8')).hexdigest()


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


def _gerar_payload_json(df_processo_data, df_status_data, df_acao_data, pipeline_nome, competencia):
    dias = sorted(
        set(df_processo_data['DATA'].unique())
        | set(df_status_data['DATA'].unique())
        | set(df_acao_data['DATA'].unique())
    )
    dados = {}
    for dia in dias:
        processo = (
            df_processo_data[df_processo_data['DATA'] == dia]
            .set_index('PROCESSO')['TOTAL']
            .to_dict()
        )
        status_chave = (
            df_status_data[df_status_data['DATA'] == dia]
            .set_index('STATUS_CHAVE')['TOTAL']
            .to_dict()
        )
        acao = (
            df_acao_data[df_acao_data['DATA'] == dia]
            .set_index('ACAO')['TOTAL']
            .to_dict()
        )
        payload_dia = {
            'processo': {k: int(v) for k, v in processo.items()},
            'status_chave': {k: int(v) for k, v in status_chave.items()},
            'classificacao_acao': {k: int(v) for k, v in acao.items()},
        }
        payload_dia['hash_conteudo'] = _hash_payload(payload_dia)
        dados[dia] = payload_dia

    return {
        'metadata': {
            'pipeline': pipeline_nome,
            'versao_metricas': '1.0.0',
            'competencia': competencia,
        },
        'orquestracao_por_dt_envio': dados,
    }


def _salvar_json(caminho, payload):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, 'w', encoding='utf-8') as arquivo:
        json.dump(payload, arquivo, ensure_ascii=False, indent=2)


def gerar_analise_dados_fase3_orquestracao(
    arquivo_dataset_orquestrado,
    raiz_analise='src/data/analise_dados',
    nome_execucao=None,
    nome_processo='orquestracao',
    pipeline_nome='complicacao_orquestracao_pipeline',
):
    _ = nome_execucao
    _ = nome_processo

    raiz_orquestracao = Path(raiz_analise) / 'orquestracao'
    raiz_lixeira = raiz_orquestracao / 'lixeira'
    raiz_orquestracao.mkdir(parents=True, exist_ok=True)

    df_usuarios = _carregar_aba_planilha(arquivo_dataset_orquestrado, 'usuarios')
    df_usuarios_resolvidos = _carregar_aba_planilha(arquivo_dataset_orquestrado, 'usuarios_resolvidos')
    mensagens = []

    df_base_processo = df_usuarios.copy()
    if len(df_usuarios_resolvidos) > 0:
        df_res = df_usuarios_resolvidos.copy()
        if 'PROCESSO' not in df_res.columns:
            df_res['PROCESSO'] = 'RESOLVIDO'
        else:
            proc_res = normalizar_texto_serie(df_res['PROCESSO'])
            df_res.loc[proc_res == '', 'PROCESSO'] = 'RESOLVIDO'
        if 'DT ENVIO' not in df_res.columns:
            df_res['DT ENVIO'] = ''
        if 'DT INTERNACAO' not in df_res.columns:
            df_res['DT INTERNACAO'] = ''
        df_base_processo = pd.concat([df_base_processo, df_res], ignore_index=True)

    comp_usuarios = _serie_competencia_internacao(df_usuarios)
    comp_processo = _serie_competencia_internacao(df_base_processo)

    if 'DT INTERNACAO' in df_usuarios.columns:
        serie_bruta = df_usuarios['DT INTERNACAO'].astype(str).str.strip()
        mask_texto = serie_bruta != ''
        mask_invalida = (comp_usuarios == 'SEM_COMPETENCIA') & mask_texto
        if mask_invalida.any():
            exemplos_invalidos = sorted(serie_bruta.loc[mask_invalida].dropna().unique().tolist())[:5]
            exemplos_txt = ', '.join(exemplos_invalidos)
            mensagens.append(
                'DT INTERNACAO com formato nao reconhecido em parte dos registros da orquestracao. '
                'Use data valida (ex.: 10/03/2026) ou MES_ANO (ex.: DEZEMBRO_2026 ou DEZEMBRO_DE_2026). '
                f'Exemplos invalidos encontrados: {exemplos_txt}'
            )
    competencias = sorted(set(comp_usuarios.unique()) | set(comp_processo.unique()))
    competencias = [c for c in competencias if c != 'SEM_COMPETENCIA']

    if not competencias:
        return {
            'ok': True,
            'pasta_saida': str(raiz_orquestracao),
            'pastas_saida': [],
            'pastas_lixeira': [],
            'arquivos_gerados': [],
            'mensagem': 'Nenhuma competencia valida de DT INTERNACAO encontrada.',
            'mensagens': mensagens,
        }

    pastas_saida = []
    backups = []
    arquivos_gerados = []

    for competencia in competencias:
        mask_u = comp_usuarios == competencia
        mask_p = comp_processo == competencia

        df_u = df_usuarios.loc[mask_u].copy()
        df_p = df_base_processo.loc[mask_p].copy()
        if len(df_u) == 0 and len(df_p) == 0:
            continue

        processo = _normalizar_processo(df_p)
        processo, valido_proc = _aplicar_regra_temporal(df_p, processo, valor_quando_invalido='PROGRAMADO')
        dt_envio_proc = _normalizar_dt_envio(df_p)

        acao = _classificar_acao(df_u)
        acao, valido_acao = _aplicar_regra_temporal(df_u, acao, valor_quando_invalido='PROGRAMADO')
        dt_envio_u = _normalizar_dt_envio(df_u)

        status_chave = _normalizar_status_chave(df_u)

        # Totais
        df_processo_total = _contagem_total(processo, 'PROCESSO')
        df_acao_total = _contagem_total(acao, 'ACAO')
        df_status_total = _contagem_total(status_chave[valido_acao], 'STATUS_CHAVE')

        # Quebra por data (somente validos temporalmente)
        df_processo_data = _contagem_por_data(
            dt_envio_proc[valido_proc],
            processo[valido_proc],
            'PROCESSO',
        )
        df_acao_data = _contagem_por_data(
            dt_envio_u[valido_acao],
            acao[valido_acao],
            'ACAO',
        )
        df_status_data = _contagem_por_data(
            dt_envio_u[valido_acao],
            status_chave[valido_acao],
            'STATUS_CHAVE',
        )

        # PROGRAMADO final em PROCESSO = PROGRAMADO existente + todos os SEM_MATCH.
        sem_match_mask = status_chave.str.upper() == 'SEM_MATCH'
        total_sem_match = int(sem_match_mask.sum())
        df_processo_total = _somar_programado_no_total(df_processo_total, total_sem_match)
        df_sem_match_data = _contagem_por_data(
            dt_envio_u[sem_match_mask],
            pd.Series('PROGRAMADO', index=dt_envio_u[sem_match_mask].index),
            'PROCESSO',
        )
        df_processo_data = _somar_programado_por_data(df_processo_data, df_sem_match_data)
        df_processo_total = _remover_chave_vazia(df_processo_total, 'PROCESSO')
        df_processo_data = _remover_chave_vazia(df_processo_data, 'PROCESSO')

        nome_pasta = _nome_pasta_competencia(competencia)
        pasta_comp = raiz_orquestracao / nome_pasta
        backup = _backup_para_lixeira(pasta_comp, raiz_lixeira)
        if backup:
            backups.append(backup)
        pasta_comp.mkdir(parents=True, exist_ok=True)

        arq_processo = pasta_comp / 'PROCESSO.csv'
        arq_processo_data = pasta_comp / 'PROCESSO_DATA.csv'
        arq_acao = pasta_comp / 'ACAO.csv'
        arq_acao_data = pasta_comp / 'ACAO_DATA.csv'
        arq_status = pasta_comp / 'STATUS_CHAVE.csv'
        arq_json = pasta_comp / _arquivo_json_competencia(competencia)

        salvar_dataframe(df_processo_total, arq_processo)
        salvar_dataframe(df_processo_data, arq_processo_data)
        salvar_dataframe(df_acao_total, arq_acao)
        salvar_dataframe(df_acao_data, arq_acao_data)
        salvar_dataframe(df_status_total, arq_status)

        payload_json = _gerar_payload_json(
            df_processo_data=df_processo_data,
            df_status_data=df_status_data,
            df_acao_data=df_acao_data,
            pipeline_nome=pipeline_nome,
            competencia=competencia,
        )
        _salvar_json(arq_json, payload_json)

        pastas_saida.append(str(pasta_comp))
        arquivos_gerados.extend(
            [
                str(arq_processo),
                str(arq_processo_data),
                str(arq_acao),
                str(arq_acao_data),
                str(arq_status),
                str(arq_json),
            ]
        )

    return {
        'ok': True,
        'pasta_saida': pastas_saida[0] if pastas_saida else str(raiz_orquestracao),
        'pastas_saida': pastas_saida,
        'pastas_lixeira': backups,
        'arquivos_gerados': arquivos_gerados,
        'mensagens': mensagens,
    }
