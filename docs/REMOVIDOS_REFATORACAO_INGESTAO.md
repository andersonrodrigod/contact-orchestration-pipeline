# Remocoes da refatoracao de ingestao

Este arquivo registra o que foi removido durante a limpeza do `ingestao_service.py`.
Sempre que uma regra sair da ingestao, registrar aqui para lembrar de revisar
outros pontos do projeto que ainda podem carregar a mesma ideia.

## 2026-05-12 - Remocao de metricas e limiar de qualidade da ingestao

Arquivo alterado:

- `src/services/ingestao_service.py`

O que foi removido da ingestao:

- calculo de quantidade de `NaT` em `Data agendamento`;
- calculo de percentual de `NaT` em `Data agendamento`;
- calculo de quantidade de `NaT` em `DT_ATENDIMENTO`;
- calculo de percentual de `NaT` em `DT_ATENDIMENTO`;
- bloqueio de saida por `limiar_nat_data`;
- retorno das chaves `nat_data_agendamento`, `pct_nat_data_agendamento`,
  `nat_dt_atendimento`, `pct_nat_dt_atendimento`;
- retorno da chave `limiar_nat_data_em_uso`;
- retorno de `qualidade_data`;
- retorno de `metricas_por_etapa` gerado pela ingestao;
- uso de `ERRO_QUALIDADE_DATA` dentro da ingestao;
- uso de `resolver_limiar_nat_data` dentro da ingestao;
- helper `_mensagem_alerta_nat`;
- estado interno `_inicializar_estado_normalizacao`.

O que ficou na ingestao:

- leitura dos arquivos;
- validacao de colunas de origem;
- padronizacao de colunas;
- normalizacao das colunas de data para datetime;
- validacao de contrato das colunas de data;
- limpeza de texto nas colunas alvo;
- criacao de `DT ENVIO`;
- salvamento dos arquivos limpos;
- erro de validacao de colunas;
- erro inesperado de ingestao.

Motivo:

- a ingestao deve ser uma etapa objetiva de limpeza e validacao estrutural;
- metricas de qualidade, limiar e observabilidade nao precisam morar nesta etapa;
- o ponto importante agora e verificar se as colunas de data existem e foram
  convertidas para tipo data.

Pontos para revisar depois:

- `core/error_codes.py`: verificar se `ERRO_QUALIDADE_DATA` ainda faz sentido para
  outros fluxos ou se deve ficar apenas no preflight.
- `src/config/governanca_config.py`: verificar se `resolver_limiar_nat_data` ainda
  e usado fora do preflight.
- `src/pipelines/preflight_pipeline.py`: ainda usa limiar e metricas de qualidade;
  decidir se o preflight deve continuar com essa responsabilidade.
- `src/pipelines/complicacao_status_pipeline.py`: ainda repassa metricas antigas
  com defaults, mesmo que a ingestao nao as retorne mais.
- `src/services/observabilidade_service.py`: ainda coleta `qualidade_data`,
  `metricas_por_etapa`, `nat_*`, `pct_nat_*` e `limiar_nat_data_em_uso` quando
  existirem.

Decisao pendente:

- remover os parametros `limiar_nat_data` e `permitir_override_limiar` das
  assinaturas publicas depois de limpar os callers.

## 2026-05-12 - Enxugamento dos logs da ingestao

Arquivo alterado:

- `src/services/ingestao_service.py`

O que foi removido/reduzido:

- logs de leitura linha a linha (`Lendo arquivo status`, quantidade de linhas e colunas);
- logs de etapa normal como `PADRONIZACAO`, `NORMALIZACAO` e `FORMATACAO`;
- logs de dtype das colunas de data;
- logs com exemplo de valor de `DT ENVIO`;
- logs com exemplo de valor de `DT_ATENDIMENTO`;
- log de `MODO` dentro da fachada `executar_ingestao_complicacao`;
- log detalhado do resultado da validacao quando ela passa.

O que ficou:

- log unico de inicio da ingestao com arquivos de entrada e saida;
- warnings quando uma validacao falha;
- warning quando a saida e bloqueada;
- log unico de sucesso com arquivos gerados;
- exception para erro inesperado;
- `logger.finalizar(...)` para manter o contrato atual do `PipelineLogger`.

Motivo:

- os logs antigos pareciam debug permanente;
- a regra da ingestao ficava escondida entre mensagens;
- a ingestao deve registrar eventos importantes, nao narrar cada linha do metodo.

Pontos para revisar depois:

- avaliar se `logger.finalizar(...)` deve continuar dentro do service ou subir para
  a camada de pipeline;
- avaliar se `PipelineLogger` deveria suportar `debug` para diagnostico opcional.

## 2026-05-12 - Remocao temporaria do fluxo somente status da ingestao

Arquivos alterados:

- `src/services/ingestao_service.py`
- `src/pipelines/complicacao_status_pipeline.py`
- `src/pipelines/complicacao_pipeline.py`
- `src/cli/modos_principais.py`
- `src/cli/modos_individuais.py`
- `src/cli/acoes_app.py`
- `src/ui/app.py`
- `src/ui/controllers/complicacao_controller.py`
- `src/ui/controllers/ingestao_controller.py`
- `src/ui/services/pipeline_runner.py`
- `src/contexts/pipeline_contextos.py`
- `src/pipelines/status_normalizar_complicacao_pipeline.py`

O que foi removido:

- `executar_ingestao_somente_status`;
- `run_complicacao_pipeline_enviar_status_somente_status`;
- `run_complicacao_pipeline_gerar_status_dataset_somente_status`;
- `run_pipeline_complicacao_somente_status`;
- modo principal `complicacao_somente_status`;
- modos individuais baseados em somente status;
- acoes app baseadas em somente status;
- loggers de contexto exclusivos de somente status;
- `run_pipeline_contexto_somente_status`.

O que foi mantido:

- fluxo principal `complicacao_com_resposta`;
- integracao/status com resposta;
- processamento completo `status + status_resposta`;
- avisos temporarios na UI quando alguem tentar usar normalizacao apenas de status.

Motivo:

- `somente_status` era uma escolha de fluxo, nao responsabilidade do `ingestao_service.py`;
- a execucao isolada de `status` e `status_resposta` deve voltar como CLI/etapa explicita;
- isso evita manter uma funcao especial dentro da ingestao para um modo que deveria ser decidido fora dela.

Pontos para recriar depois:

- CLI para normalizar apenas `status`;
- CLI para normalizar apenas `status_resposta`;
- CLI para normalizar os dois juntos;
- testes dedicados para cada CLI;
- reativar UI usando essas etapas novas.
