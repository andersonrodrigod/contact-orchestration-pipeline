from src.utils.arquivos import ler_arquivo_csv, salvar_output_validacao
from src.services.normalizacao_services import (
    formatar_coluna_data_br,
    limpar_texto_exceto_colunas,
    normalizar_tipos_dataframe,
)
from src.services.schema_service import (
    padronizar_colunas_status,
    padronizar_colunas_status_resposta,
)
from src.services.validacao_service import (
    validar_colunas_origem_para_padronizacao,
    validar_padronizacao_colunas_data,
)
from src.services.dataset_service import concatenar_status_resposta_eletivo_internacao


def _executar_normalizacao_padronizacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
    mensagens_iniciais=None,
):
    if mensagens_iniciais is None:
        mensagens_iniciais = []

    df_status = ler_arquivo_csv(arquivo_status)
    df_status_resposta = ler_arquivo_csv(arquivo_status_resposta)

    resultado_colunas_origem = validar_colunas_origem_para_padronizacao(
        df_status, df_status_resposta
    )
    if not resultado_colunas_origem['ok']:
        resultado_final = {
            'ok': False,
            'mensagens': mensagens_iniciais + resultado_colunas_origem['mensagens'],
        }
        salvar_output_validacao(resultado_final, output_validacao)
        return resultado_final

    df_status = padronizar_colunas_status(df_status)
    df_status_resposta = padronizar_colunas_status_resposta(df_status_resposta)

    df_status = normalizar_tipos_dataframe(df_status, colunas_data=['DT_ENVIO'])
    df_status_resposta = normalizar_tipos_dataframe(df_status_resposta, colunas_data=['DT_ATENDIMENTO'])

    df_status = limpar_texto_exceto_colunas(df_status, colunas_ignorar=['DT_ENVIO'])
    df_status_resposta = limpar_texto_exceto_colunas(df_status_resposta, colunas_ignorar=['DT_ATENDIMENTO'])

    resultado_validacao = validar_padronizacao_colunas_data(df_status, df_status_resposta)
    resultado_final = {
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
    salvar_output_validacao(resultado_final, output_validacao)

    formatar_coluna_data_br(df_status, 'DT_ENVIO')
    formatar_coluna_data_br(df_status_resposta, 'DT_ATENDIMENTO')

    df_status.to_csv(saida_status, sep=';', index=False, encoding='utf-8-sig')
    df_status_resposta.to_csv(saida_status_resposta, sep=';', index=False, encoding='utf-8-sig')

    return resultado_final


def run_ingestao_complicacao(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_complicacao='src/data/status_resposta_complicacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
):
    return _executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_complicacao,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        output_validacao=output_validacao,
        mensagens_iniciais=['Modo complicacao selecionado.'],
    )


def run_ingestao_unificar(
    arquivo_status='src/data/status.csv',
    arquivo_status_resposta_eletivo='src/data/status_respostas_eletivo.csv',
    arquivo_status_resposta_internacao='src/data/status_resposta_internacao.csv',
    arquivo_status_resposta_unificado='src/data/status_resposta_eletivo_internacao.csv',
    saida_status='src/data/arquivo_limpo/status_limpo.csv',
    saida_status_resposta='src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv',
    output_validacao='src/data/arquivo_limpo/output_validacao_datas.txt',
):
    resultado_concat = concatenar_status_resposta_eletivo_internacao(
        arquivo_eletivo=arquivo_status_resposta_eletivo,
        arquivo_internacao=arquivo_status_resposta_internacao,
        arquivo_saida=arquivo_status_resposta_unificado,
    )

    if not resultado_concat['ok']:
        salvar_output_validacao(resultado_concat, output_validacao)
        return resultado_concat

    return _executar_normalizacao_padronizacao(
        arquivo_status=arquivo_status,
        arquivo_status_resposta=arquivo_status_resposta_unificado,
        saida_status=saida_status,
        saida_status_resposta=saida_status_resposta,
        output_validacao=output_validacao,
        mensagens_iniciais=resultado_concat['mensagens'],
    )
