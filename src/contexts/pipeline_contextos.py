from dataclasses import dataclass

from src.config.paths import DEFAULTS_COMPLICACAO


@dataclass(frozen=True)
class PipelineContexto:
    nome: str
    defaults: dict
    logger_status_com_resposta: str
    logger_criacao_dataset: str
    logger_orquestracao: str


CONTEXTO_PIPELINE_COMPLICACAO = PipelineContexto(
    nome='complicacao',
    defaults=DEFAULTS_COMPLICACAO,
    logger_status_com_resposta='status_complicacao_pipeline',
    logger_criacao_dataset='criacao_dataset_complicacao',
    logger_orquestracao='orquestracao_complicacao',
)
