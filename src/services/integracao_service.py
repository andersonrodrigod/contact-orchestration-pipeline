from pathlib import Path
from uuid import uuid4

from src.services.status_service import integrar_status_com_resposta
from src.utils.arquivos import ler_arquivo_csv


def filtrar_status_por_hsm(arquivo_status, hsms_permitidos, arquivo_status_filtrado):
    df_status = ler_arquivo_csv(arquivo_status)
    total_antes = len(df_status)

    if 'HSM' not in df_status.columns:
        df_status.to_csv(arquivo_status_filtrado, sep=';', index=False, encoding='utf-8-sig')
        return {'total_antes': total_antes, 'total_depois': len(df_status)}

    hsms_permitidos_set = {str(h).strip() for h in hsms_permitidos}
    mask = df_status['HSM'].astype(str).str.strip().isin(hsms_permitidos_set)
    df_filtrado = df_status[mask].copy()
    df_filtrado.to_csv(arquivo_status_filtrado, sep=';', index=False, encoding='utf-8-sig')
    return {'total_antes': total_antes, 'total_depois': len(df_filtrado)}


def integrar_com_filtro_hsm(
    arquivo_status,
    arquivo_status_resposta,
    arquivo_saida,
    hsms_permitidos,
    colunas_limpar=None,
):
    pasta_saida = Path(arquivo_saida).parent
    pasta_saida.mkdir(parents=True, exist_ok=True)
    arquivo_status_filtrado = str(
        pasta_saida / f"_tmp_status_filtrado_integracao_{uuid4().hex}.csv"
    )

    try:
        resumo_filtro = filtrar_status_por_hsm(
            arquivo_status=arquivo_status,
            hsms_permitidos=hsms_permitidos,
            arquivo_status_filtrado=arquivo_status_filtrado,
        )

        resultado = integrar_status_com_resposta(
            arquivo_status=arquivo_status_filtrado,
            arquivo_status_resposta=arquivo_status_resposta,
            arquivo_saida=arquivo_saida,
            colunas_limpar=colunas_limpar,
        )
        return {**resultado, 'resumo_filtro': resumo_filtro}
    finally:
        Path(arquivo_status_filtrado).unlink(missing_ok=True)


def integrar_somente_status_com_filtro_hsm(
    arquivo_status,
    arquivo_saida,
    hsms_permitidos,
    colunas_limpar=None,
):
    pasta_saida = Path(arquivo_saida).parent
    pasta_saida.mkdir(parents=True, exist_ok=True)
    arquivo_status_filtrado = str(
        pasta_saida / f"_tmp_status_filtrado_status_somente_{uuid4().hex}.csv"
    )

    try:
        resumo_filtro = filtrar_status_por_hsm(
            arquivo_status=arquivo_status,
            hsms_permitidos=hsms_permitidos,
            arquivo_status_filtrado=arquivo_status_filtrado,
        )

        df_status = ler_arquivo_csv(arquivo_status_filtrado)
        if 'Contato' in df_status.columns:
            df_status['Contato'] = df_status['Contato'].astype(str).str.strip()
            df_status['NOME_MANIPULADO'] = df_status['Contato'].astype(str).str.split('_', n=1).str[0].str.strip()
        else:
            df_status['NOME_MANIPULADO'] = ''

        df_status['RESPOSTA'] = 'Sem resposta'

        if colunas_limpar is None:
            colunas_limpar = []
        for coluna in colunas_limpar:
            if coluna in df_status.columns:
                df_status[coluna] = ''

        df_status.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')
        return {
            'ok': True,
            'arquivo_saida': arquivo_saida,
            'total_status': len(df_status),
            'com_match': 0,
            'sem_match': len(df_status),
            'resumo_filtro': resumo_filtro,
        }
    finally:
        Path(arquivo_status_filtrado).unlink(missing_ok=True)
