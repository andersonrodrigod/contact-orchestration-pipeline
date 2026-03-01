from pathlib import Path

from core.logger import PipelineLogger
from src.services.ingestao_service import executar_ingestao_somente_status
from src.utils.arquivos import ler_arquivo_csv


def run_status_normalizar_internacao_eletivo_pipeline(
    arquivo_status='src/data/status.csv',
    arquivo_status_normalizado='src/data/arquivo_limpo/status_internacao_eletivo_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status_sem_internacao_eletivo.csv',
    nome_logger='ingestao_individual_excluir_internacao_eletivo',
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    resultado_ingestao = executar_ingestao_somente_status(
        arquivo_status=arquivo_status,
        saida_status=arquivo_status_normalizado,
        nome_logger=nome_logger,
        logger=logger,
    )
    if not resultado_ingestao.get('ok'):
        logger.finalizar('FALHA_INGESTAO')
        return resultado_ingestao

    df_status = ler_arquivo_csv(arquivo_status_normalizado)
    if 'HSM' not in df_status.columns:
        logger.finalizar('FALHA_VALIDACAO_HSM')
        return {
            'ok': False,
            'mensagens': ['Coluna HSM nao encontrada no status normalizado para exclusao.'],
        }

    hsms_excluir = {'Pesquisa_Pos_cir_urg_intern', 'Pesquisa_Pos_cir_eletivo'}
    total_antes = len(df_status)
    mask_manter = ~df_status['HSM'].astype(str).str.strip().isin(hsms_excluir)
    df_filtrado = df_status[mask_manter].copy()
    total_depois = len(df_filtrado)
    total_excluido = total_antes - total_depois

    Path(arquivo_saida).parent.mkdir(parents=True, exist_ok=True)
    df_filtrado.to_csv(arquivo_saida, sep=';', index=False, encoding='utf-8-sig')

    resultado = {
        'ok': True,
        'arquivo_saida': arquivo_saida,
        'total_antes': total_antes,
        'total_depois': total_depois,
        'total_excluido': total_antes - total_depois,
        'mensagens': [
            'Status normalizado e filtrado por exclusao de HSM com sucesso.',
            f'total_antes={total_antes}',
            f'total_depois={total_depois}',
            f'total_excluido={total_excluido}',
        ],
    }
    logger.finalizar('SUCESSO')
    return resultado
