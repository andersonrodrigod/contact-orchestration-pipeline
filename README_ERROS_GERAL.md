# README Geral de Erros, Riscos e Correcoes

Este arquivo consolida os relatorios de bugs em uma visao unica e continua.
Objetivo: centralizar historico, status atual e plano de acao.

## Escopo

1. Codigo em `src/` (legacy fora do foco principal).
2. Erros de regra de negocio, qualidade de dados e contratos de integracao.
3. Status de cada item: `RESOLVIDO`, `PARCIAL`, `ABERTO`, `ADIADO`.

## Historico consolidado

Fontes migradas:
1. `RELATORIO_BUGS_ESCALA_1_2.md`.
2. `RELATORIO_BUGS_ESCALA_2_2026-03-05.md`.

## Itens por prioridade e status

### Alta prioridade

0. Persistencia de arquivos mesmo com falha de validacao de dados
- Status: `RESOLVIDO`

1. Criacao de dataset sem tratamento global de excecao e sem `codigo_erro` padrao
- Status: `RESOLVIDO`

2. Regra redundante/ambigua de segundo envio (`SEGUNDO_ENVIO_LIDO`)
- Status: `RESOLVIDO`

3. Duplicidade mascarada de colunas essenciais (`P1.1`, `P2.1`, `P3.1`, `P4.1`)
- Status: `RESOLVIDO`

### Media prioridade

0. Falha silenciosa no enriquecimento de abas secundarias do dataset
- Status: `RESOLVIDO`

1. Regra de qualidade de data inconsistente entre modos de ingestao
- Status: `RESOLVIDO`

2. Preflight pode aprovar sem validar data de `status_resposta`
- Status: `RESOLVIDO`

3. Falta de tratamento de excecao em `executar_ingestao_unificar`
- Status: `RESOLVIDO`

4. Baixa observabilidade de descartes por data invalida na integracao
- Status: `RESOLVIDO`

5. Limiar de qualidade de data fixo e sem parametrizacao operacional
- Status: `RESOLVIDO`

6. Contrato estrito de colunas obrigatorias na criacao de dataset
- Status: `PARCIAL` (decisao de produto)
- Situacao: continua bloqueando quando falta coluna critica, com erro claro.
- Justificativa: regra foi mantida para proteger qualidade da base.

7. Preflight de internacao/eletivo dependia do arquivo unificado previo
- Status: `RESOLVIDO`

### Baixa prioridade / tecnica

1. Execucao adicional XLSX usa criterio parcial de disponibilidade
- Status: `PARCIAL` (decisao de produto)
- Situacao atual:
  - O fluxo geral nao roda mais rodada XLSX adicional automaticamente.
  - XLSX ficou restrito ao modo individual, apenas nos fluxos de concatenacao/unificacao definidos.
- Decisao de produto:
  - Manter separacao explicita entre execucao geral e execucao individual para reduzir ambiguidade operacional.
- Impacto residual:
  - Ainda pode existir combinacao mista CSV/XLSX se o operador montar entradas heterogeneas no modo individual.
- Proxima acao recomendada:
  - Opcional: bloquear formato misto no mesmo passo individual ou exigir escolha explicita do formato.

2. Aba `disparo` pode conter registro sem telefone de disparo
- Status: `PARCIAL` (decisao de produto)
- Situacao:
  - A aba `disparo` mantem registros elegiveis por processo mesmo quando `TELEFONE DISPARO` fica vazio.
- Justificativa da decisao:
  - Produto optou por manter rastreabilidade operacional dos elegiveis, mesmo sem telefone valido para envio imediato.
- Impacto:
  - A aba pode conter linhas nao disparaveis no ciclo atual.
- Mitigacao atual:
  - `VALIDACAO FINAL` permite separar rapidamente o que esta apto para disparo.
- Proxima acao recomendada:
  - Opcional: flag no app para exportar apenas disparaveis (`TELEFONE DISPARO` valido).

3. Duplicidade por `CHAVE RELATORIO` na aba `disparo`
- Status: `RESOLVIDO` (decisao de produto)
- Situacao:
  - A deduplicacao por `CHAVE RELATORIO` e intencional para impedir reenvio ao mesmo usuario/chave no mesmo ciclo.
- Regra adotada:
  - Manter apenas uma linha por `CHAVE RELATORIO` na aba `disparo`.
- Observacao:
  - Comportamento alinhado com a regra de negocio de anti-duplicidade de disparo.

4. Validacao de chave na aba `disparo` e funcional, mas pouco discriminante
- Status: `PARCIAL`
- Situacao:
  - `VALIDACAO CHAVE` compara contra a base de usuarios do mesmo arquivo final.
- Impacto:
  - Em condicao normal tende a gerar muitos `OK`, com baixo poder diagnostico.
- Proxima acao recomendada:
  - Evoluir para validacao contra referencia externa ou manter apenas como auditoria basica.

5. Parse de data com risco de compatibilidade de versao (`format='mixed'`)
- Status: `RESOLVIDO`

6. Warning de parse ISO com `dayfirst=True` em normalizacao de datas
- Status: `RESOLVIDO`

7. Execucao concorrente pode corromper XLSX de saida (`BadZipFile`)
- Status: `PARCIAL` (nao aplicavel no fluxo oficial do app)
- Contexto de produto:
  - No modo ambos (execucao geral), o app aceita apenas CSV.
  - XLSX fica restrito a fluxos individuais de concatenacao/unificacao, com escolha explicita de formato pelo usuario.
- Impacto residual:
  - O risco permanece apenas para execucao manual/externa em paralelo escrevendo no mesmo arquivo de saida.
- Proxima acao recomendada:
  - Opcional: lock de arquivo se houver necessidade de suportar concorrencia fora do fluxo oficial.

8. Estrategia avancada de padronizacao total antes do processo
- Status: `ADIADO` (feature futura)

## Decisoes de negocio registradas

1. `P1` permanece obrigatoria para criacao/segmentacao de dataset.
2. Sinonimos de resposta (`SIM/NAO`) nao serao alterados agora.
3. Politica para data invalida: falha em cenario extremo e manter observabilidade dos descartes.
4. Na aba `disparo`, manter registros elegiveis mesmo sem `TELEFONE DISPARO` valido (com `VALIDACAO FINAL`) por decisao de produto.
5. Separacao de formatos: comportamento XLSX focado em modo individual para reduzir risco no fluxo geral.
6. Deduplicacao por `CHAVE RELATORIO` na aba `disparo` e obrigatoria para evitar reenvio ao mesmo usuario/chave.

## Runbook rapido para operacao

1. Se falhar por colunas faltantes: corrigir layout de origem e reprocessar.
2. Se falhar por coluna mascarada (`P1.1` etc.): remover/renomear duplicata na fonte.
3. Se cair qualidade de data: verificar `%NaT` e ajustar origem/limiar.
4. Se `sem_match` subir: verificar descartes por data invalida primeiro.

## Governanca deste arquivo

1. Novos bugs entram aqui com status e data.
2. Evitar novos relatorios soltos para o mesmo escopo.
3. Quando item mudar de status, atualizar este arquivo e citar commit relacionado.
