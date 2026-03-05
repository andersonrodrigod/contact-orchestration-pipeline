from dataclasses import dataclass

from src.config.paths import DEFAULTS_COMPLICACAO, DEFAULTS_INTERNACAO_ELETIVO


@dataclass(frozen=True)
class PipelineContexto:
    nome: str
    defaults: dict
    logger_status_com_resposta: str
    logger_status_somente_status: str
    logger_criacao_dataset: str
    logger_criacao_dataset_somente_status: str
    logger_orquestracao: str
    logger_orquestracao_somente_status: str


CONTEXTO_PIPELINE_COMPLICACAO = PipelineContexto(
    nome='complicacao',
    defaults=DEFAULTS_COMPLICACAO,
    logger_status_com_resposta='status_complicacao_pipeline',
    logger_status_somente_status='status_complicacao_somente_status_pipeline',
    logger_criacao_dataset='criacao_dataset_complicacao',
    logger_criacao_dataset_somente_status='criacao_dataset_complicacao_somente_status',
    logger_orquestracao='orquestracao_complicacao',
    logger_orquestracao_somente_status='orquestracao_complicacao_somente_status',
)


CONTEXTO_PIPELINE_INTERNACAO_ELETIVO = PipelineContexto(
    nome='internacao_eletivo',
    defaults=DEFAULTS_INTERNACAO_ELETIVO,
    logger_status_com_resposta='status_internacao_eletivo_pipeline',
    logger_status_somente_status='status_internacao_eletivo_somente_status_pipeline',
    logger_criacao_dataset='criacao_dataset_internacao_eletivo',
    logger_criacao_dataset_somente_status='criacao_dataset_internacao_eletivo_somente_status',
    logger_orquestracao='orquestracao_internacao_eletivo',
    logger_orquestracao_somente_status='orquestracao_internacao_eletivo_somente_status',
)
