from src.pipelines.finalizacao_pipeline import run_finalizacao_pipeline


def run_complicacao_pipeline_finalizar(
    arquivo_dataset_status='src/data/arquivo_limpo/complicacao_status.xlsx',
    arquivo_saida_final='src/data/arquivo_limpo/complicacao_final.xlsx',
    nome_logger='finalizacao_complicacao',
):
    return run_finalizacao_pipeline(
        arquivo_dataset_entrada=arquivo_dataset_status,
        arquivo_dataset_saida=arquivo_saida_final,
        nome_logger=nome_logger,
    )
