from pathlib import Path

from core.logger import PipelineLogger
from src.services.status_service import integrar_status_com_resposta
from src.utils.arquivos import ler_arquivo_csv


def _filtrar_status_por_hsm(arquivo_status, hsms_permitidos, arquivo_status_filtrado):
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


def _integrar_com_filtro_hsm(
    arquivo_status,
    arquivo_status_resposta,
    arquivo_saida,
    hsms_permitidos,
    nome_logger,
    colunas_limpar=None,
):
    logger = PipelineLogger(nome_pipeline=nome_logger)
    logger.info('MODO', f'Integracao iniciada com HSM permitidos={hsms_permitidos}')
    logger.info('INICIO', f'arquivo_status={arquivo_status}')
    logger.info('INICIO', f'arquivo_status_resposta={arquivo_status_resposta}')
    logger.info('INICIO', f'arquivo_saida={arquivo_saida}')

    try:
        pasta_saida = Path(arquivo_saida).parent
        pasta_saida.mkdir(parents=True, exist_ok=True)
        arquivo_status_filtrado = str(pasta_saida / '_tmp_status_filtrado_integracao.csv')

        resumo_filtro = _filtrar_status_por_hsm(
            arquivo_status=arquivo_status,
            hsms_permitidos=hsms_permitidos,
            arquivo_status_filtrado=arquivo_status_filtrado,
        )
        logger.info('FILTRO_HSM', f"status antes={resumo_filtro['total_antes']}")
        logger.info('FILTRO_HSM', f"status depois={resumo_filtro['total_depois']}")

        resultado = integrar_status_com_resposta(
            arquivo_status=arquivo_status_filtrado,
            arquivo_status_resposta=arquivo_status_resposta,
            arquivo_saida=arquivo_saida,
            colunas_limpar=colunas_limpar,
        )

        logger.info('MATCH', f"total_status={resultado['total_status']}")
        logger.info('MATCH', f"com_match={resultado['com_match']}")
        logger.info('MATCH', f"sem_match={resultado['sem_match']}")
        logger.finalizar('SUCESSO')

        Path(arquivo_status_filtrado).unlink(missing_ok=True)
        return resultado
    except Exception as erro:
        logger.exception('ERRO_EXECUCAO', erro)
        logger.finalizar('ERRO')
        return {'ok': False, 'mensagens': [str(erro)]}


def integrar_dados_status_complicacao(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _integrar_com_filtro_hsm(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa Complicacoes Cirurgicas', 'Pesquisa Complica\u00e7\u00f5es Cirurgicas'],
        nome_logger='integracao_complicacao',
        colunas_limpar=[
            'Conta',
            'Mensagem',
            'Categoria',
            'Template',
            'Agendamento',
            'Status agendamento',
            'Agente',
        ],
    )


def integrar_dados_status_unificar(
    arquivo_status='src/data/arquivo_limpo/status_limpo.csv',
    arquivo_status_resposta='src/data/arquivo_limpo/status_resposta_eletivo_internacao_limpo.csv',
    arquivo_saida='src/data/arquivo_limpo/status.csv',
):
    return _integrar_com_filtro_hsm(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta,
        arquivo_saida=arquivo_saida,
        hsms_permitidos=['Pesquisa_Pos_cir_urg_intern', 'Pesquisa_Pos_cir_eletivo'],
        nome_logger='integracao_unificar_execucao',
        colunas_limpar=[],
    )
