# METRICAS PARA ANALISE DE DADOS

## Contexto
Este documento consolida o plano inicial para:
1. Corrigir comportamento funcional da orquestracao.
2. Definir metricas de analise sem comprometer o pipeline atual.
3. Planejar um novo pipeline dedicado para consolidacao de dados analiticos.

Regra desta etapa:
- Nenhuma alteracao de codigo foi planejada neste documento.
- Este README serve para destrinchar requisitos, riscos e etapas.

## Objetivo 1 - Resultado atual da planilha

### 1.1 Correcao obrigatoria (bug funcional)
Problema identificado:
- A logica atual da aba `disparo` move registros para disparo e remove da aba `usuarios`.

Correcao desejada:
- Registros enviados para `disparo` devem permanecer tambem na aba `usuarios`.
- A aba `disparo` passa a funcionar como visao/recorte operacional, sem efeito de exclusao em `usuarios`.

### 1.2 Dados pedidos para analise (estado atual)
Fonte principal: arquivo final da orquestracao.

Metricas solicitadas:
1. Quantidade total de `Nao quis` na coluna `STATUS` do arquivo que cria o dataset.
2. Quantidade total de resolvidos:
   - total de linhas da aba `usuarios_resolvidos` no arquivo final de orquestra.
3. Quantidade total de cada valor em `PROCESSO`:
   - considerando a aba `usuarios` no arquivo final de orquestra.

### 1.3 Resultado esperado
- Essas metricas devem alimentar graficos (definicao visual sera feita em etapa futura).
- Nesta fase, o foco e garantir consistencia numerica e rastreabilidade das contagens.

## Objetivo 2 - Novo pipeline para envios individuais (pos-orquestracao)

### 2.1 Proposta geral
Criar pipeline novo, separado da finalizacao principal, para gerar uma nova aba de envios individuais com foco em processo.

Nome sugerido (provisorio):
- `pipeline_envios_individuais`

### 2.2 Campos obrigatorios da nova aba
1. `TELEFONE PRIORIDADE`:
   - mesma logica da coluna existente em `usuarios`.
   - usada como eixo de rastreio dos valores associados ao telefone.
2. Colunas base:
   - `COD USUARIO`
   - `USUARIO`
   - `CHAVE RELATORIO`
   - `CHAVE STATUS`
3. Colunas de agregacao/estado:
   - `LIDA_RESPOSTA_SIM`
   - `LIDA_RESPOSTA_NAO`
   - `LIDA_SEM_RESPOSTA`
   - `LIDA`
   - `ENTREGUE`
   - `ENVIADA`
   - `NAO_ENTREGUE_META`
   - `MENSAGEM_NAO_ENTREGUE`
   - `EXPERIMENTO`
   - `OPT_OUT`

### 2.3 Regra da nova coluna `FINAL`
Regra solicitada:
1. Se `ACAO == SEM_CONTATO_DISPONIVEL` -> `FINAL = SEM_CONTATO_DISPONIVEL`.
2. Caso contrario -> `FINAL = EM_ANDAMENTO`.

## Objetivo 3 - Correcao de bug no `status.csv`

Escopo de contagem solicitado:
1. Quantidade total de linhas de `status.csv`.
2. Quantidade total de cada valor distinto da coluna `RESPOSTA`.
3. Quantidade total de cada valor distinto da coluna `Status`.
4. Quantidade total de cada valor da coluna `Status` por data:
   - usar `DT ENVIO` ou `Data agendamento`.
5. Quantidade total de cada valor da coluna `RESPOSTA` por data:
   - usar `DT ENVIO` ou `Data agendamento`.
6. Quantidade total consolidada dos itens acima por data.

## Proposta de implementacao em etapas

### Etapa A - Correcoes de consistencia (antes de metricas)
1. Corrigir regra de persistencia da aba `usuarios` x `disparo`.
2. Validar que nenhuma contagem regressiva indevida ocorre apos a correcao.

### Etapa B - Camada de metricas (sem grafico ainda)
1. Criar funcoes puras de calculo das metricas do Objetivo 1.
2. Criar funcoes puras de calculo das metricas de `status.csv`.
3. Salvar resultado em tabela estruturada (CSV/JSON) para auditoria.

### Etapa C - Novo pipeline de envios individuais
1. Implementar geracao da nova aba com colunas obrigatorias.
2. Implementar regra da coluna `FINAL`.
3. Validar consistencia por amostragem e por totais.

### Etapa D - Visualizacao (futura)
1. Escolher biblioteca de grafico (seaborn/matplotlib).
2. Ligar graficos nas metricas validadas da Etapa B/C.
3. Publicar layout final dos paineis/figuras.

## Riscos de comprometer o codigo e mitigacoes

Risco 1: alterar regra de `disparo` quebrar fluxo de orquestracao.
- Mitigacao: manter comportamento antigo protegido por teste de regressao e validar contagens antes/depois.

Risco 2: mistura de regra de negocio com regra analitica.
- Mitigacao: separar pipeline operacional do pipeline de metricas/envios_individuais.

Risco 3: divergencia de contagens por coluna/data devido a normalizacao de datas.
- Mitigacao: definir uma unica coluna data de referencia por relatorio e documentar fallback.

Risco 4: crescimento de complexidade sem contrato de saida.
- Mitigacao: definir schema fixo das metricas (nomes, tipos, granularidade temporal).

## Pontos para discutir na proxima conversa
1. Qual coluna data sera oficial para analise (`DT ENVIO` ou `Data agendamento`) e qual sera fallback.
2. Se `Nao quis` deve vir de `STATUS` bruto ou status ja padronizado.
3. Granularidade dos graficos: diario, semanal e mensal.
4. Se a nova aba de envios individuais sera por linha de usuario ou por linha de telefone.
5. Se o pipeline de metricas deve rodar junto da orquestracao ou separado por comando proprio.
