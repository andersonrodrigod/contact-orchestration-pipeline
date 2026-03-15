# README - Geracao de Graficos e Slides

## Objetivo
Padronizar a geracao de graficos a partir dos dados analiticos do pipeline, com dois modos:

1. Automatico: executou o processo, gerou imagem.
2. Manual (Utilitario): escolher arquivo/pasta e gerar para inspecao futura.

Este desenho permite validar os graficos antes de ativar a geracao automatica de slides.

## Principios
- Separar geracao de imagem da geracao de slide.
- Manter saidas organizadas por contexto e etapa do processo.
- Nunca bloquear o pipeline principal por falha de grafico.
- Permitir reprocessamento manual no frame Utilitario.

## Estrutura de pastas
Saidas de imagem:

- `src/data/analise_dados/imagens/complicacao/uniao_status_resposta`
- `src/data/analise_dados/imagens/complicacao/resumo_complicacao`
- `src/data/analise_dados/imagens/complicacao/status_enviado`
- `src/data/analise_dados/imagens/complicacao/orquestracao`
- `src/data/analise_dados/imagens/internacao/uniao_status_resposta`
- `src/data/analise_dados/imagens/internacao/resumo_internacao`
- `src/data/analise_dados/imagens/internacao/status_enviado`
- `src/data/analise_dados/imagens/internacao/orquestracao`

Saidas de apresentacao:

- `src/data/analise_dados/apresentacoes`

## Etapas que geram dados para graficos
1. Uniao de Status e Flow de Respostas (fase 1)
2. Resumo (complicacao/internacao)
3. Envio de Status (fase 2)
4. Orquestracao (fase 3)

## Plano de implementacao
### Fase 1 - Imagens automaticas
- Ao finalizar cada etapa acima, gerar PNGs correspondentes.
- Salvar `manifest.json` na pasta da etapa com:
  - quais graficos foram gerados
  - quais foram ignorados (arquivo faltando, coluna ausente, etc.)
  - mensagens de validacao

### Fase 2 - Utilitario (inspecao manual)
Criar frame(s) no Utilitario para:
- Selecionar contexto (`complicacao` ou `internacao`)
- Selecionar etapa (`uniao_status_resposta`, `resumo`, `status_enviado`, `orquestracao`)
- Informar arquivo/pasta de entrada
- Gerar imagens sob demanda

Objetivo: facilitar auditoria e ajuste visual sem depender de nova execucao completa.

### Fase 3 - Slides
- Inicialmente manual (botao no Utilitario: "Gerar Slides").
- Depois opcionalmente automatico ao fim do processo.
- Slide sempre consome imagens prontas (nao recalcula metricas).

## Nomenclatura recomendada dos arquivos PNG
Padrao:

`<etapa>_<metrica>_<contexto>.png`

Exemplos:
- `uniao_status_resposta_status_por_data_complicacao.png`
- `resumo_complicacao_video_sim_por_dia_complicacao.png`
- `status_enviado_qt_soma_colunas_internacao.png`
- `orquestracao_classificacao_acao_por_data_internacao.png`

## Comportamento esperado em dados parciais
- Se existir somente `complicacao`, gerar somente `complicacao`.
- Se existir somente `internacao`, gerar somente `internacao`.
- Falha em um grafico nao derruba os demais.

## Decisao atual
Ordem de entrega acordada:

1. Geracao automatica de imagens por processo.
2. Frames no Utilitario para geracao manual e inspecao.
3. Slides (manual primeiro, automatico depois de validacao visual).
