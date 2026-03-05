# README Geral de Erros, Riscos e Correções

Este arquivo consolida os antigos relatórios de bugs em uma visão única e contínua.
Objetivo: centralizar histórico, status atual e plano de ação sem espalhar informação em múltiplos arquivos.

## Escopo

1. Código em `src/` (legacy fora do foco principal).
2. Erros de regra de negócio, qualidade de dados e contratos de integração.
3. Status de cada item: `RESOLVIDO`, `PARCIAL`, `ABERTO`, `ADIADO`.

## Histórico consolidado

Fontes migradas:
1. `RELATORIO_BUGS_ESCALA_1_2.md` (auditoria inicial).
2. `RELATORIO_BUGS_ESCALA_2_2026-03-05.md` (revisão posterior).

## Itens por prioridade e status

### Alta prioridade

1. Regra redundante/ambígua de segundo envio (`SEGUNDO_ENVIO_LIDO`)
- Status: `RESOLVIDO`
- Situação anterior:
  - Regra podia ficar morta por depender de coluna não confiável no contrato.
- Correção aplicada:
  - Regra `SEGUNDO_ENVIO_LIDO` removida.
  - Fluxo mantém apenas `SEGUNDO_ENVIO` via `LIDA_SEM_RESPOSTA == 1`.
- Evidência:
  - Busca em `src/` sem ocorrência de `SEGUNDO_ENVIO_LIDO`.

2. Duplicidade mascarada de colunas essenciais (`P1.1`, `P2.1`, `P3.1`, `P4.1`)
- Status: `RESOLVIDO`
- Situação anterior:
  - `pandas` renomeava duplicadas com sufixo `.<numero>` e a validação exata não barrava.
- Correção aplicada:
  - Validação explícita para padrão `^P[1-4]\.\d+$`.
  - Retorno técnico novo: `colunas_mascaradas_duplicadas`.
- Resultado:
  - Pipeline bloqueia com mensagem acionável antes de seguir.

### Média prioridade

3. Baixa observabilidade de descartes por data inválida na integração
- Status: `RESOLVIDO`
- Situação anterior:
  - Descarte ocorria, mas sem exposição consistente no pipeline final.
- Correção aplicada:
  - Contadores expostos em retorno e mensagens:
    - `descartados_status_data_invalida`
    - `descartados_resposta_data_invalida`
  - Log final das etapas de integração com ambos os contadores.

4. Limiar de qualidade de data fixo e sem parametrização operacional
- Status: `RESOLVIDO`
- Situação anterior:
  - Regra fixa dificultava ajuste por ambiente/contexto.
- Correção aplicada:
  - Parâmetro `limiar_nat_data` adicionado nas funções de ingestão.
  - Valor padrão preservado em `30.0`.
  - Log explícito do limiar em uso.

5. Contrato estrito de colunas obrigatórias na criação de dataset
- Status: `PARCIAL` (decisão de produto)
- Situação:
  - Continua bloqueando quando falta coluna crítica (com mensagem clara).
- Motivo do status:
  - Comportamento foi mantido por decisão de negócio.
- Próximo passo:
  - Evoluir para pre-flight avançado no app (feature futura).

6. Preflight de internação/eletivo dependia do arquivo unificado prévio
- Status: `RESOLVIDO`
- Situação detectada nos testes de refatoração:
  - O modo de preflight podia falhar se `status_resposta_eletivo_internacao.csv` ainda não existisse.
- Correção aplicada:
  - Fallback para concatenação em memória de:
    - `status_resposta_eletivo.csv`
    - `status_resposta_internacao.csv`
- Resultado:
  - Preflight de `internacao_eletivo` funciona mesmo sem etapa de unificação prévia.

### Baixa prioridade / técnica

6. Parse de data com risco de compatibilidade de versão (`format='mixed'`)
- Status: `RESOLVIDO`
- Correção aplicada:
  - Dependência removida.
  - Fallback mantido para formatos alternativos.
- Observação:
  - Pode haver warning de inferência em alguns cenários mistos, sem quebrar execução.

7. Warning de parse ISO com `dayfirst=True` em normalização de datas
- Status: `RESOLVIDO`
- Situação detectada:
  - `UserWarning` do pandas ao parsear formatos `%Y-%m-%d %H:%M:%S` com `dayfirst=True`.
- Correção aplicada:
  - Separação do parse por padrão:
    - ISO com `dayfirst=False`
    - não-ISO com `dayfirst=True`
    - fallback final para formatos alternativos
- Resultado:
  - Warning eliminado sem alteração das métricas finais nos testes de regressão.

8. Execução concorrente pode corromper XLSX de saída (BadZipFile)
- Status: `ABERTO` (risco operacional)
- Situação detectada:
  - Rodadas paralelas escrevendo o mesmo `*.xlsx` de saída podem gerar:
    - `BadZipFile: Bad CRC-32 for file 'docProps/core.xml'`
- Causa provável:
  - Concorrência de escrita sobre o mesmo arquivo final.
- Impacto:
  - Falha intermitente em orquestração/finalização quando duas execuções compartilham destinos.
- Mitigação atual:
  - Evitar rodar modos que escrevem os mesmos arquivos em paralelo.
  - Executar fluxos completos em série.
- Próxima solução recomendada:
  - Adotar lock de arquivo por destino ou diretório de saída por execução (com timestamp/uuid).

7. Estratégia avançada de padronização total antes do processo
- Status: `ADIADO` (feature futura)
- Planejado:
  - Pipeline de pre-flight.
  - Normalização completa de headers/tipos/datas antes das regras de negócio.
- Referência:
  - `README_FEATURES_FUTURAS.md`
  - `README_REFATORACAO_SUGESTOES.md`

## Decisões de negócio registradas

1. `P1` permanece obrigatória para criação/segmentação de dataset.
2. Item de sinônimos para resposta (`SIM/NAO`) não será alterado agora.
3. Política de integração com data inválida:
  - falha em cenário extremo conforme regra do serviço;
  - manter observabilidade dos descartes para decisão operacional.

## Runbook rápido para operação

1. Se falhar por colunas faltantes:
  - corrigir layout de origem e reprocessar.
2. Se falhar por coluna mascarada (`P1.1` etc.):
  - remover/renomear duplicata na fonte.
3. Se cair qualidade de data:
  - checar `%NaT` e ajustar a origem;
  - usar `limiar_nat_data` adequado no contexto.
4. Se `sem_match` subir:
  - verificar contadores de descartes por data inválida primeiro.

## Governança deste arquivo

1. Novos bugs entram aqui, com status e data.
2. Evitar novos relatórios soltos para o mesmo escopo.
3. Quando item mudar de status, atualizar este arquivo e citar commit relacionado.
