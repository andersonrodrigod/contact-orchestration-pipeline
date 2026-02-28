from pathlib import Path

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
    arquivo_status_filtrado = str(pasta_saida / '_tmp_status_filtrado_integracao.csv')

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
