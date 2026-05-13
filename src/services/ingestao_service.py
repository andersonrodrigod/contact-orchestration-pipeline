from core.logger import PipelineLogger
from core.error_codes import (
    ERRO_INGESTAO,
    ERRO_VALIDACAO_COLUNAS,
)
from src.services.normalizacao_services import (
    criar_coluna_dt_envio_por_data_agendamento,
    formatar_coluna_data_br,
    limpar_texto_colunas_alvo,
    normalizar_tipos_dataframe,
)
from src.services.padronizacao_service import (
    padronizar_colunas_status,
    padronizar_colunas_status_resposta,
)
from src.services.schema_resposta_service import garantir_contrato_resposta_canonica
from src.services.validacao_service import (
    validar_colunas_origem_para_padronizacao,
    validar_padronizacao_colunas_data,
)
from src.utils.arquivos import ler_arquivo_csv, salvar_dataframe


# Colunas textuais que passam por limpeza. A diferenca de caixa em RESPOSTA/resposta
# existe porque status e status_resposta chegam com contratos diferentes.
COLUNAS_TEXTO_ALVO_STATUS = ['HSM', 'Status', 'Respondido', 'RESPOSTA']
COLUNAS_TEXTO_ALVO_STATUS_RESPOSTA = ['HSM', 'Status', 'Respondido', 'resposta']


def _ler_arquivos_status(arquivo_status, arquivo_status_resposta):
    # Primeira leitura fisica do fluxo com resposta: status e status_resposta.
    # A funcao apenas le e devolve os DataFrames crus.
    df_status = ler_arquivo_csv(arquivo_status)
    df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)
    return df_status, df_status_resposta


def _validar_colunas_origem_normalizacao(
    logger,
    df_status,
    df_status_resposta,
    modo_estrito_alias_resposta,
    janela_corte_alias_resposta_ciclos,
):
    # Validacao antes de renomear colunas. Aqui o codigo ainda olha para os
    # nomes originais e aliases possiveis do arquivo recebido.
    resultado_colunas_origem = validar_colunas_origem_para_padronizacao(
        df_status,
        df_status_resposta,
        modo_estrito_alias_resposta=modo_estrito_alias_resposta,
        janela_corte_alias_resposta_ciclos=janela_corte_alias_resposta_ciclos,
    )
    return resultado_colunas_origem


def _aplicar_padronizacao_status_e_resposta(df_status, df_status_resposta):
    # Padroniza os dois DataFrames para o contrato interno esperado pelas
    # proximas etapas. No status_resposta tambem garante a coluna canonica
    # de resposta depois da padronizacao.
    df_status = padronizar_colunas_status(df_status)
    df_status_resposta = padronizar_colunas_status_resposta(df_status_resposta)
    garantir_contrato_resposta_canonica(
        df_status_resposta,
        contexto='ingestao.status_resposta_pos_padronizacao',
    )
    return df_status, df_status_resposta


def _normalizar_tipos_data(
    df_status,
    df_status_resposta,
):
    # Converte as colunas de data para datetime. A verificacao de contrato
    # acontece depois em validar_padronizacao_colunas_data.
    df_status = normalizar_tipos_dataframe(df_status, colunas_data=['Data agendamento'])
    df_status_resposta = normalizar_tipos_dataframe(
        df_status_resposta, colunas_data=['DT_ATENDIMENTO']
    )
    return df_status, df_status_resposta


def _montar_resultado_normalizacao(
    mensagens_iniciais,
    resultado_colunas_origem,
    resultado_validacao,
):
    # Junta mensagens em um dicionario padrao de retorno. Este resultado e usado
    # pela pipeline acima para decidir se continua ou para.
    return {
        'ok': (
            resultado_colunas_origem['ok']
            and resultado_validacao['ok']
        ),
        'mensagens': (
            mensagens_iniciais
            + resultado_colunas_origem['mensagens']
            + resultado_validacao['mensagens']
        ),
    }


def executar_normalizacao_padronizacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    limiar_nat_data=None,
    contexto=None,
    permitir_override_limiar=True,
    modo_estrito_alias_resposta=None,
    janela_corte_alias_resposta_ciclos=None,
    mensagens_iniciais=None,
    logger=None,
    finalizar_logger=True,
):
    # Fluxo principal de ingestao quando existem status e status_resposta.
    # Esta funcao le arquivos, valida, padroniza, normaliza, limpa texto,
    # cria colunas temporariamente mantidas por compatibilidade, salva saidas
    # e transforma excecoes em resultado padrao.
    if mensagens_iniciais is None:
        mensagens_iniciais = []
    if logger is None:
        logger = PipelineLogger()
    logger.info(
        'INGESTAO_INICIO',
        (
            f'arquivo_status={arquivo_status}; '
            f'arquivo_status_resposta={arquivo_status_resposta}; '
            f'saida_status={saida_status}; '
            f'saida_status_resposta={saida_status_resposta}'
        ),
    )

    etapa_atual = 'INICIO'
    try:
        # 1) Le os dois arquivos de entrada.
        etapa_atual = 'LEITURA_STATUS'
        df_status, df_status_resposta = _ler_arquivos_status(
            arquivo_status, arquivo_status_resposta
        )

        # 2) Valida colunas originais antes de qualquer renomeacao.
        resultado_colunas_origem = _validar_colunas_origem_normalizacao(
            logger,
            df_status,
            df_status_resposta,
            modo_estrito_alias_resposta,
            janela_corte_alias_resposta_ciclos,
        )
        if not resultado_colunas_origem['ok']:
            resultado_final = {
                'ok': False,
                'mensagens': mensagens_iniciais + resultado_colunas_origem['mensagens'],
                'codigo_erro': ERRO_VALIDACAO_COLUNAS,
            }
            logger.warning('VALIDACAO_ORIGEM', 'Falhou validacao de colunas de origem')
            if finalizar_logger:
                logger.finalizar('FALHA_VALIDACAO_ORIGEM')
            return resultado_final

        # 3) Padroniza nomes/contratos para o formato interno do pipeline.
        etapa_atual = 'PADRONIZACAO'
        df_status, df_status_resposta = _aplicar_padronizacao_status_e_resposta(
            df_status, df_status_resposta
        )

        # 4) Converte datas para datetime.
        etapa_atual = 'NORMALIZACAO_TIPOS'
        df_status, df_status_resposta = _normalizar_tipos_data(
            df_status,
            df_status_resposta,
        )

        # 5) Limpa texto apenas nas colunas alvo, sem varrer o DataFrame inteiro.
        etapa_atual = 'LIMPEZA_TEXTO'
        df_status = limpar_texto_colunas_alvo(
            df_status,
            colunas_alvo=COLUNAS_TEXTO_ALVO_STATUS,
        )
        df_status_resposta = limpar_texto_colunas_alvo(
            df_status_resposta,
            colunas_alvo=COLUNAS_TEXTO_ALVO_STATUS_RESPOSTA,
        )

        # 6) Cria DT ENVIO no status a partir de Data agendamento.
        etapa_atual = 'CRIAR_DT_ENVIO'
        criar_coluna_dt_envio_por_data_agendamento(df_status)

        # 7) Valida o contrato final das colunas de data depois das transformacoes.
        etapa_atual = 'VALIDACAO_DATA'
        resultado_validacao = validar_padronizacao_colunas_data(df_status, df_status_resposta)
        resultado_final = _montar_resultado_normalizacao(
            mensagens_iniciais=mensagens_iniciais,
            resultado_colunas_origem=resultado_colunas_origem,
            resultado_validacao=resultado_validacao,
        )
        if not resultado_final['ok']:
            # Se houver erro de validacao, o arquivo de saida nao e salvo.
            resultado_final['codigo_erro'] = ERRO_VALIDACAO_COLUNAS
            status_final = 'FALHA_VALIDACAO_DATA'

            motivo_principal = (
                resultado_final['mensagens'][-1] if len(resultado_final['mensagens']) > 0 else 'Motivo nao informado.'
            )
            mensagem_bloqueio = (
                f"Saida bloqueada na etapa={etapa_atual}: arquivos nao foram salvos. "
                f"codigo_erro={resultado_final['codigo_erro']}. Motivo: {motivo_principal}"
            )
            logger.warning('BLOQUEIO_SAIDA', mensagem_bloqueio)
            resultado_final['mensagens'] = resultado_final['mensagens'] + [mensagem_bloqueio]
            if finalizar_logger:
                logger.finalizar(status_final)
            return resultado_final

        # 8) So depois de aprovado, formata a data de atendimento para exibicao BR.
        etapa_atual = 'FORMATAR_DT_ATENDIMENTO'
        formatar_coluna_data_br(df_status_resposta, 'DT_ATENDIMENTO')

        # 9) Ultima etapa: escreve os arquivos limpos no disco.
        etapa_atual = 'SALVAR_ARQUIVOS'
        salvar_dataframe(df_status, saida_status)
        salvar_dataframe(df_status_resposta, saida_status_resposta)
        logger.info(
            'INGESTAO_SUCESSO',
            f'saida_status={saida_status}; saida_status_resposta={saida_status_resposta}',
        )

        if finalizar_logger:
            logger.finalizar('SUCESSO')
        return resultado_final
    except Exception as erro:
        # Qualquer erro inesperado vira retorno padrao de ingestao, com a etapa
        # onde falhou para facilitar diagnostico.
        logger.exception('ERRO_EXECUCAO', erro)
        resultado_erro = {
            'ok': False,
            'mensagens': [f'Erro inesperado na execucao (etapa={etapa_atual}): {type(erro).__name__}: {erro}'],
            'codigo_erro': ERRO_INGESTAO,
        }
        if finalizar_logger:
            logger.finalizar('ERRO')
        return resultado_erro


def executar_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    limiar_nat_data=None,
    permitir_override_limiar=True,
    modo_estrito_alias_resposta=None,
    janela_corte_alias_resposta_ciclos=None,
    executar_xlsx_adicional=False,
    logger=None,
):
    # Fachada publica usada pela pipeline de complicacao. Ela executa o fluxo
    # CSV principal. O parametro executar_xlsx_adicional permanece na assinatura
    # para compatibilidade com chamadas existentes, mas nao aciona mais um fluxo
    # paralelo em XLSX dentro da ingestao.
    logger_externo = logger is not None
    if logger is None:
        logger = PipelineLogger(nome_pipeline='ingestao_complicacao')

    # Execucao principal: status CSV + status_resposta CSV.
    resultado = executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        limiar_nat_data=limiar_nat_data,
        contexto='complicacao',
        permitir_override_limiar=permitir_override_limiar,
        modo_estrito_alias_resposta=modo_estrito_alias_resposta,
        janela_corte_alias_resposta_ciclos=janela_corte_alias_resposta_ciclos,
        mensagens_iniciais=['Modo complicacao selecionado.'],
        logger=logger,
        finalizar_logger=not logger_externo,
    )
    if not resultado.get('ok'):
        return resultado

    return resultado


