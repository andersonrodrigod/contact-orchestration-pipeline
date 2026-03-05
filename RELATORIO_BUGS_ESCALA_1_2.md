# Relatorio de Bugs e Riscos - Auditoria (Escala 1, 2 e 3)

Data: 2026-03-02
Branch: chore/bug-audit-escala-1-2
Escopo: codigo do projeto em `src/` (legacy fora do foco)
Metodo: analise estatica de codigo (sem alterar regra de negocio nem gerar saidas do pipeline)

## Escala 1 (alto impacto)

### 1) Regra nova de orquestracao pode nunca disparar
- Local: `src/services/orquestracao_service.py:87`, `src/services/orquestracao_service.py:105`
- Evidencia: a regra usa a coluna `SEGUNDO_ENVIO`, mas essa coluna nao aparece no schema final.
- Contrato relacionado: `src/config/schemas.py:43-95` (nao existe `SEGUNDO_ENVIO` em `COLUNAS_FINAIS_DATASET`).
- Risco: a condicao `s_lida_envio == 1` tende a ficar sempre zero e o processo `SEGUNDO_ENVIO_LIDO` pode nunca ser aplicado.
- Porque pode dar erro: regra de negocio fica "morta" silenciosamente, sem aviso de que nao foi calculada.
- Sugestao:
  - calcular `SEGUNDO_ENVIO` antes da classificacao (pipeline de contagens), ou
  - trocar a regra para uma coluna que de fato existe no dataset, ou
  - falhar explicitamente quando a coluna esperada nao existir.

### 2) Integracao falha por 1 data invalida e derruba o lote inteiro
- Local: `src/services/status_service.py:10-15`, `src/services/status_service.py:45-48`
- Evidencia: qualquer `NaT` em `DT ENVIO` ou `DT_ATENDIMENTO` gera `ValueError`.
- Risco: 1 linha ruim interrompe 100% da integracao.
- Porque pode dar erro: em ambiente real e comum haver linhas sujas; a falha total reduz resiliencia operacional.
- Sugestao:
  - adicionar modo tolerante: descartar linhas invalidas com contador de rejeitadas,
  - manter modo estrito apenas por flag/config.

### 3) Inconsistencia de contrato em status de chave
- Local: `src/services/dataset_service.py:204`, `src/services/dataset_service.py:272`
- Evidencia: match principal marca `OK`; fallback (que e match valido) marca `ERROR`.
- Risco: consumidor pode interpretar fallback como erro real e remover/filtrar registro valido.
- Porque pode dar erro: semantica de negocio e observabilidade ficam ambiguidas.
- Sugestao:
  - usar valores explicitos: `OK_PRINCIPAL`, `OK_FALLBACK`, `SEM_MATCH`.

## Escala 2 (medio impacto)

### 4) Falha de contagens quando nao ha match por telefone, mesmo havendo match por chave
- Local: `src/services/dataset_metricas_service.py:128-142`
- Evidencia: se `df_join_envio_ok` ficar vazio, retorna erro e interrompe enriquecimento.
- Risco: pipeline pode falhar desnecessariamente em cenarios com `CHAVE STATUS` valido e telefone ausente/inconsistente.
- Porque pode dar erro: o proprio codigo ja calcula contagem por chave sem telefone (`147-159`), mas nao chega nessa etapa.
- Sugestao:
  - degradar para warning e seguir com contagens por chave,
  - falhar apenas quando nao houver nenhum match util (nem por chave).

### 5) Regras de validacao e regras de segmentacao duplicadas e acopladas a `P1`
- Local: `src/config/schemas.py:23-40`, `src/services/dataset_service.py:469-481`, `src/services/dataset_service.py:521-535`
- Evidencia: `P1` e obrigatoria no schema e depois e validada novamente em outra etapa.
- Risco: aumento de acoplamento e falso-negativo para layouts com variacao controlada.
- Porque pode dar erro: qualquer pequena mudanca de layout quebra em dois pontos e gera manutencao redundante.
- Sugestao:
  - centralizar obrigatoriedade em um unico ponto,
  - separar "obrigatorio para criar dataset" de "obrigatorio para segmentacao de respondidos".

### 6) Dependencia de `format='mixed'` no parse de data
- Local: `src/services/normalizacao_services.py:76-80`
- Evidencia: `pd.to_datetime(..., format='mixed')` depende de versao especifica de pandas.
- Risco: quebra em ambientes com pandas mais antigo.
- Porque pode dar erro: `TypeError` em runtime por parametro nao suportado.
- Sugestao:
  - remover `format='mixed'` e usar parse padrao com `dayfirst=True`,
  - ou fixar versao minima de pandas no projeto.
- Atualizacao (2026-03-05):
  - Correcao aplicada no codigo: `format='mixed'` removido.
  - Parse permanece com `errors='coerce'` e `dayfirst=True`, com fallback para formatos ISO/alternativos.
  - Validacao local (amostras reais do projeto): sem diferenca de resultado frente ao comportamento anterior.

### 7) Regra de qualidade permite ate 30% de datas invalidas sem bloquear
- Local: `src/services/ingestao_service.py:20`, `src/services/ingestao_service.py:91-126`, `src/services/ingestao_service.py:234-251`
- Evidencia: limite fixo `30.0%` para `NaT`.
- Risco: ate 30% de linhas podem seguir com dado degradado e afetar match/classificacao.
- Porque pode dar erro: problema passa como sucesso parcial e aparece apenas no resultado final.
- Sugestao:
  - tornar limite configuravel por ambiente,
  - registrar contagem de descartes e falhar com thresholds menores em producao.
- Atualizacao (2026-03-05):
  - Correcao aplicada no codigo: quando `% NaT >= 30%` em `Data agendamento` ou `DT_ATENDIMENTO`, a etapa retorna `ok=False` (falha).

## Escala 3 (baixo impacto / qualidade)

### 8) Constante nao utilizada
- Local: `src/services/orquestracao_service.py:10-15`
- Evidencia: `ABAS_PADRAO` nao e usada; `ABAS_OBRIGATORIAS_FINALIZACAO` e usada.
- Risco: confusao de manutencao e chance de divergencia futura.
- Sugestao: remover constante morta ou unificar em uma unica fonte.

### 9) Padronizacao de resposta lida cobre poucos sinonimos
- Local: `src/services/dataset_metricas_service.py:37-45`
- Evidencia: aceita apenas `sim/s`, `nao/n`, `sem resposta`.
- Risco: respostas validas com variacoes comuns podem cair em vazio.
- Sugestao: ampliar dicionario de sinonimos e logar valores nao mapeados.

## Priorizacao recomendada (ordem de ataque)
1. Corrigir regra `SEGUNDO_ENVIO` (Escala 1).
2. Tornar integracao de datas resiliente (Escala 1).
3. Ajustar contrato de `STATUS CHAVE` (Escala 1).
4. Degradar falha de contagens por telefone para fallback por chave (Escala 2).
5. Reduzir acoplamento de validacoes com `P1` e revisar threshold de qualidade de datas (Escala 2).

## Observacao de teste
- Este relatorio foi gerado por auditoria estatica de codigo.
- Nao foram executados testes de integracao com massa real neste passo.
