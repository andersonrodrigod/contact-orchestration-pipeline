from src.pipelines.orquestracao_base_pipeline import executar_orquestracao_pipeline


def run_internacao_eletivo_pipeline_orquestrar(
    arquivo_dataset_status='src/data/arquivo_limpo/internacao_status.xlsx',
    arquivo_saida_final='src/data/arquivo_limpo/internacao_final.xlsx',
    nome_logger='orquestracao_internacao_eletivo',
):
    return executar_orquestracao_pipeline(
        arquivo_dataset_entrada=arquivo_dataset_status,
        arquivo_dataset_saida=arquivo_saida_final,
        nome_logger=nome_logger,
    )
