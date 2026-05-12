# Relatorio de funcionamento e plano de refatoracao - foco complicacao

Data do mapeamento: 2026-05-08

## Objetivo

Este relatorio mapeia como o projeto executa hoje do inicio ao fim, com foco no modo `complicacao`, e indica onde comecar a refatoracao da chave, da numeracao e da classificacao.

A conclusao principal: a refatoracao deve comecar em `src/services/dataset_service.py`, especialmente na funcao `_enriquecer_dataset_com_status`. A orquestracao final usa o resultado dessa etapa, mas a chave, o telefone enviado, a prioridade, o proximo telefone e as contagens nascem antes, na criacao do dataset status.

## Entrada principal

Arquivo inicial:

- `main.py`

Fluxo:

1. `main.py` monta o registro de modos em `_obter_registro_modos`.
2. `run_pipeline(modo='ambos_com_resposta')` procura o modo solicitado.
3. Se o modo existir em `MODOS_PRINCIPAIS`, executa a funcao ligada ao modo.
4. Se for modo agregado (`ambos_com_resposta`, `ambos_somente_status`, `ambos`), chama `executar_modo_ambos`.
5. Ao final, anexa codigo de erro, registra historico e imprime resumo.

Modos principais de complicacao:

- `complicacao_com_resposta`
- `complicacao`
- `complicacao_somente_status`
- `complicacao_gerar_status_dataset`
- `complicacao_orquestracao`
- `preflight_complicacao`

Arquivos de configuracao que definem caminhos e contexto:

- `src/config/paths.py`
- `src/contexts/pipeline_contextos.py`
- `src/contexts/integracao_contextos.py`

## Separacao atual entre complicacao e internacao

Hoje existe separacao por contexto, mas ainda ha bastante codigo compartilhado.

Complicacao usa:

- `DEFAULTS_COMPLICACAO`
- `CONTEXTO_PIPELINE_COMPLICACAO`
- `CONTEXTO_INTEGRACAO_COMPLICACAO`
- `src/pipelines/complicacao_pipeline.py`
- `src/pipelines/complicacao_status_pipeline.py`
- `src/pipelines/complicacao_orquestracao_pipeline.py`

Internacao/eletivo usa:

- `DEFAULTS_INTERNACAO_ELETIVO`
- `CONTEXTO_PIPELINE_INTERNACAO_ELETIVO`
- `CONTEXTO_INTEGRACAO_INTERNACAO_ELETIVO`
- `src/pipelines/internacao_eletivo_pipeline.py`
- `src/pipelines/internacao_eletivo_status_pipeline.py`
- `src/pipelines/internacao_eletivo_orquestracao_pipeline.py`

Codigo compartilhado entre os dois:

- `src/services/ingestao_service.py`
- `src/services/integracao_service.py`
- `src/services/status_service.py`
- `src/services/dataset_service.py`
- `src/services/dataset_metricas_service.py`
- `src/services/orquestracao_service.py`
- `src/services/normalizacao_services.py`
- `src/services/texto_service.py`
- `src/services/validacao_service.py`
- `src/pipelines/contexto_pipeline_core.py`
- `src/pipelines/contexto_status_pipeline_base.py`
- `src/pipelines/join_status_resposta_pipeline.py`
- `src/pipelines/orquestracao_base_pipeline.py`

Observacao importante: `internacao_eletivo_status_pipeline.py` tambem chama `criar_dataset_complicacao`. Isso mostra que o nome da funcao esta acoplado ao historico do projeto, mas ela virou uma funcao generica de dataset para os dois contextos. Antes de apagar internacao numa branch, e melhor extrair essa funcao para um nome neutro.

## Ordem de execucao - complicacao com resposta

Modo:

```bash
python main.py --modo complicacao_com_resposta
```

Ordem resumida:

1. `main.py`
2. `run_pipeline('complicacao_com_resposta')`
3. `run_pipeline_complicacao_com_resposta`
4. `run_complicacao_pipeline`
5. `run_pipeline_contexto_com_resposta`
6. `run_complicacao_pipeline_gerar_status_dataset`
7. `run_complicacao_pipeline_enviar_status_com_resposta`
8. `executar_ingestao_complicacao`
9. `run_unificar_status_resposta_complicacao_pipeline`
10. `gerar_analise_dados_fase1_csv`
11. `gerar_graficos_uniao_status_resposta`
12. `run_complicacao_pipeline_criar_dataset_status`
13. `run_criacao_dataset_status_base`
14. `criar_dataset_complicacao`
15. `gerar_resumo_complicacao_csv`
16. `gerar_tabela_resumo_dia_complicacao`
17. `gerar_analise_dados_fase2_csv`
18. `gerar_graficos_status_enviado`
19. `run_complicacao_pipeline_orquestrar`
20. `executar_orquestracao_pipeline`
21. `gerar_dataset_final`
22. `gerar_analise_dados_fase3_orquestracao`
23. `gerar_graficos_orquestracao`

## Fase 0 - Preflight

Modo:

```bash
python main.py --modo preflight_complicacao
```

Arquivo principal:

- `src/pipelines/preflight_pipeline.py`

Funcao:

- `run_preflight_complicacao`

O que valida:

- existencia de arquivos de entrada;
- colunas obrigatorias do status e status_resposta;
- colunas obrigatorias do dataset origem;
- qualidade de datas (`Data agendamento` e `DT_ATENDIMENTO`);
- limite de `NaT` configurado em `governanca_config`.

Recomendacao: antes de refatorar a chave, manter o preflight como primeiro portao. Ele deve continuar passando para garantir que a mudanca esta mexendo na regra, nao na estrutura basica dos arquivos.

## Fase 1 - Ingestao e uniao status + resposta

Arquivo principal:

- `src/pipelines/complicacao_status_pipeline.py`

Funcao de entrada:

- `run_complicacao_pipeline_enviar_status_com_resposta`

Ela chama:

- `executar_ingestao_complicacao`
- `run_unificar_status_resposta_complicacao_pipeline`
- `gerar_analise_dados_fase1_csv`
- `gerar_graficos_uniao_status_resposta`

### Ingestao

Arquivo:

- `src/services/ingestao_service.py`

Funcoes relevantes:

- `executar_ingestao_complicacao`
- `executar_normalizacao_padronizacao`
- `_ler_arquivos_status`
- `_validar_colunas_origem_normalizacao`
- `_aplicar_padronizacao_status_e_resposta`
- `_normalizar_tipos_e_coletar_qualidade_data`

Entradas:

- `src/data/status.csv`
- `src/data/status_resposta_complicacao.csv`

Saidas:

- `src/data/arquivo_limpo/status_complicacao_limpo.csv`
- `src/data/arquivo_limpo/status_resposta_complicacao_limpo.csv`

Papel na chave:

- ainda nao decide a chave final da orquestracao;
- normaliza colunas, texto, data e resposta;
- cria `DT ENVIO` a partir de `Data agendamento`;
- prepara os dados que depois serao usados no match.

### Integracao status + resposta

Arquivos:

- `src/pipelines/join_status_resposta_pipeline.py`
- `src/services/integracao_service.py`
- `src/services/status_service.py`

Funcoes:

- `run_unificar_status_resposta_complicacao_pipeline`
- `_run_unificar_status_resposta_pipeline`
- `integrar_com_filtro_hsm`
- `filtrar_status_por_hsm`
- `integrar_status_com_resposta`

O que faz:

- filtra o status por HSM de complicacao;
- cruza status com status_resposta;
- usa data como chave temporaria de integracao;
- preserva status mesmo com data invalida, mas resposta invalida pode ser descartada;
- gera o status integrado.

Saida:

- `src/data/arquivo_limpo/status_complicacao.csv`

Papel na chave:

- ainda nao gera `CHAVE STATUS` do dataset;
- prepara `Contato`, `Telefone`, `Status`, `Respondido`, `resposta`, `DT ENVIO`;
- esses campos alimentam o lookup da proxima fase.

## Fase 2 - Criacao do dataset status

Arquivo principal:

- `src/services/dataset_service.py`

Funcao de entrada:

- `criar_dataset_complicacao`

Essa e a fase mais importante para a refatoracao da chave.

Ordem interna:

1. Le `arquivo_complicacao`.
2. Valida colunas obrigatorias.
3. Valida colunas de segmentacao (`STATUS`, `P1`).
4. Carrega status integrado com `_carregar_status_para_lookup`.
5. Prepara contagens com `preparar_contagens_status`.
6. Monta a aba `usuarios` com `_montar_df_final_complicacao`.
7. Enriquece `usuarios` com `_enriquecer_dataset_com_status`.
8. Monta base de `usuarios_respondidos`.
9. Enriquece `usuarios_respondidos` com `_enriquecer_dataset_com_status`.
10. Cria `usuarios_resolvidos` vazio.
11. Persiste `complicacao_status.xlsx`.

### Onde a chave nasce hoje

Dentro de `_montar_df_final_complicacao`:

- coluna origem `CHAVE` vira `CHAVE RELATORIO`;

Dentro de `_enriquecer_dataset_com_status`:

- inicia `CHAVE STATUS` como vazio;
- inicia `STATUS CHAVE` como `SEM_MATCH`;
- tenta match principal por `CHAVE RELATORIO -> Contato`;
- se encontrar, define:
  - `CHAVE STATUS = CHAVE RELATORIO`
  - `STATUS CHAVE = OK_PRINCIPAL`
  - `TELEFONE ENVIADO`
  - `ULTIMO STATUS DE ENVIO`
  - `DT ENVIO`
  - `RESPOSTA`
  - `IDENTIFICACAO`
- se nao encontrar, tenta fallback por `USUARIO + TELEFONE 1..5 -> NOME_MANIPULADO + Telefone`;
- se fallback encontrar, define:
  - `CHAVE STATUS = Contato`
  - `STATUS CHAVE = OK_FALLBACK`
  - `TELEFONE ENVIADO`
  - demais campos de status.

### Onde a numeracao/telefone acontece hoje

Ainda em `_enriquecer_dataset_com_status`:

- identifica qual `TELEFONE 1..5` bate com `TELEFONE ENVIADO`;
- escreve `TELEFONE PRIORIDADE`;
- escreve `STATUS TELEFONE`;
- marca historico de envio por chave em `TELEFONE STATUS 1..5`;
- chama `_preencher_telefone_prioridade_fallback`;
- chama `_definir_proximo_telefone_disponivel`.

### Onde as contagens acontecem hoje

Arquivo:

- `src/services/dataset_metricas_service.py`

Funcoes:

- `preparar_contagens_status`
- `aplicar_contagens_status`
- `_normalizar_status_para_contagens`
- `_preencher_contagens_status_mapeado`
- `_preencher_contagens_lida_resposta`

Essas funcoes contam por `CHAVE STATUS`, usando o status integrado. Geram colunas:

- `LIDA`
- `ENTREGUE`
- `ENVIADA`
- `NAO_ENTREGUE_META`
- `MENSAGEM_NAO_ENTREGUE`
- `EXPERIMENTO`
- `OPT_OUT`
- `LIDA_RESPOSTA_SIM`
- `LIDA_RESPOSTA_NAO`
- `LIDA_SEM_RESPOSTA`
- versoes `QT ...`
- `QT TELEFONES`

Ponto de refatoracao: numeracao e classificacao deveriam ser calculadas a partir de um mesmo objeto de contexto da chave, nao por colunas soltas no dataframe.

## Fase 3 - Orquestracao final

Arquivos:

- `src/pipelines/complicacao_orquestracao_pipeline.py`
- `src/pipelines/orquestracao_base_pipeline.py`
- `src/services/orquestracao_service.py`

Ordem:

1. `run_complicacao_pipeline_orquestrar`
2. `executar_orquestracao_pipeline`
3. `gerar_dataset_final`
4. `aplicar_classificacao_processo_acao` em `usuarios`
5. `aplicar_classificacao_processo_acao` em `usuarios_respondidos`
6. `orquestrar_usuarios_respondidos`
7. `_criar_aba_disparo`
8. salva `complicacao_final.xlsx`
9. gera analise fase 3
10. gera graficos de orquestracao

### Onde a classificacao acontece hoje

Funcao:

- `aplicar_classificacao_processo_acao`

Ela consome:

- `STATUS CHAVE`
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
- `RESPOSTA`
- `PROXIMO TELEFONE DISPONIVEL`
- `TELEFONE PRIORIDADE`

Ela produz:

- `PROCESSO`
- `ACAO`

Ponto importante: a classificacao depende totalmente das colunas geradas antes. Por isso, se a chave mudar, a classificacao precisa ser protegida com testes antes da refatoracao.

## Relacao com analise de dados

Analise de dados e graficos rodam em tres fases:

### Fase 1

Depois de unir status + resposta:

- `gerar_analise_dados_fase1_csv`
- `gerar_graficos_uniao_status_resposta`

### Fase 2

Depois de criar `complicacao_status.xlsx`:

- `gerar_resumo_complicacao_csv`
- `gerar_tabela_resumo_dia_complicacao`
- `gerar_analise_dados_fase2_csv`
- `gerar_graficos_status_enviado`

### Fase 3

Depois de criar `complicacao_final.xlsx`:

- `gerar_analise_dados_fase3_orquestracao`
- `gerar_graficos_orquestracao`

Se voce quer analise seria por valor da chave, a fase 2 e a fase 3 dependem de uma chave mais explicita, estavel e auditavel.

## Onde comecar a refatoracao

Ordem recomendada:

### 1. Criar testes de caracterizacao antes de mudar comportamento

Comecar pelos testes, porque hoje a refatoracao mexe no centro do pipeline.

Arquivos sugeridos:

- `tests/test_chave_orquestracao_service.py`
- `tests/test_dataset_service_chave.py`
- `tests/test_orquestracao_service.py`

Cenarios minimos:

- match principal por `CHAVE RELATORIO`;
- fallback por `USUARIO + telefone`;
- sem match;
- chave com senha no ultimo ligador;
- telefone enviado em `TELEFONE 1`;
- telefone enviado em `TELEFONE 2..5`;
- proximo telefone disponivel;
- sem telefone disponivel;
- contagem `LIDA`, `LIDA_RESPOSTA_SIM`, `LIDA_RESPOSTA_NAO`, `LIDA_SEM_RESPOSTA`;
- processo e acao esperados para cada combinacao.

### 2. Extrair regra da chave para um servico novo

Arquivo novo sugerido:

- `src/services/chave_orquestracao_service.py`

Responsabilidades:

- normalizar chave;
- extrair senha da chave, se existir;
- separar chave base e chave unica;
- resolver match principal;
- resolver match fallback;
- devolver um resultado unico e auditavel.

Exemplo de retorno desejado:

```python
{
    'chave_relatorio': '...',
    'chave_status': '...',
    'status_chave': 'OK_PRINCIPAL',
    'senha_chave': '...',
    'origem_match': 'CHAVE_RELATORIO',
}
```

### 3. Extrair regra de telefone para um servico novo

Arquivo novo sugerido:

- `src/services/telefone_orquestracao_service.py`

Responsabilidades:

- normalizar telefones;
- descobrir `TELEFONE ENVIADO`;
- descobrir `TELEFONE PRIORIDADE`;
- descobrir `PROXIMO TELEFONE DISPONIVEL`;
- preencher `TELEFONE STATUS 1..5`;
- retornar a numeracao junto com a chave.

### 4. Unificar contagem e classificacao em uma camada de decisao

Arquivo novo sugerido:

- `src/services/decisao_orquestracao_service.py`

Responsabilidades:

- receber chave resolvida;
- receber historico/contagens de status;
- receber situacao dos telefones;
- calcular `PROCESSO`;
- calcular `ACAO`;
- devolver resultado final.

Isso permitiria que numeracao e classificacao acontecessem juntas, como voce descreveu.

### 5. Substituir `_enriquecer_dataset_com_status` por uma funcao orquestradora menor

Hoje `_enriquecer_dataset_com_status` faz muitas coisas:

- match por chave;
- fallback por telefone;
- preenche colunas de status;
- calcula telefone prioridade;
- calcula proximo telefone;
- marca historico;
- aplica contagens;
- calcula metricas.

Depois da extracao, ela deveria virar apenas coordenadora:

```python
resolver_chave(...)
resolver_telefones(...)
calcular_contagens(...)
classificar_decisao(...)
aplicar_resultado_no_dataframe(...)
```

### 6. Depois renomear `criar_dataset_complicacao`

So depois de estabilizar o comportamento:

- renomear para algo neutro, como `criar_dataset_status_orquestracao`;
- deixar alias temporario `criar_dataset_complicacao = criar_dataset_status_orquestracao`;
- atualizar internacao apenas se necessario.

## Estrategia de branches

Voce comentou em ter uma branch so complicacao e outra so internacao. Eu recomendo esta ordem:

1. Criar branch de trabalho para foco em complicacao:
   - `codex/refatoracao-chave-complicacao`
2. Nessa branch, nao deletar internacao no primeiro passo.
3. Criar testes de caracterizacao da complicacao.
4. Extrair servicos novos sem alterar o comportamento final.
5. Quando complicacao estiver estavel, criar uma branch de congelamento da internacao:
   - `codex/snapshot-internacao`
6. So depois avaliar se vale manter duas linhas de codigo ou apenas dois contextos configurados.

Motivo: internacao esta parada, mas ainda serve como alerta contra acoplamentos escondidos. Se deletar cedo, voce perde a chance de descobrir o que era compartilhado por acidente.

## Arquivos prioritarios para refatorar

Ordem pratica:

1. `tests/test_dataset_metricas_service.py`
2. `tests/test_orquestracao_service.py`
3. novo `tests/test_chave_orquestracao_service.py`
4. novo `src/services/chave_orquestracao_service.py`
5. novo `src/services/telefone_orquestracao_service.py`
6. novo `src/services/decisao_orquestracao_service.py`
7. `src/services/dataset_service.py`
8. `src/services/dataset_metricas_service.py`
9. `src/services/orquestracao_service.py`
10. `src/pipelines/complicacao_status_pipeline.py`

## Riscos atuais

### Testes nao estao verdes

A suite atual rodou com falhas em normalizacao de texto e uma falha de baseline por arquivo ausente. Antes da refatoracao grande, separar testes unitarios de testes que dependem de dados gerados.

### Dados gerados misturados com repositorio

Ha arquivos deletados em `src/data/analise_dados/.../lixeira/...` no `git status`. Antes de criar branch de refatoracao, decidir se essas delecoes sao intencionais.

### Nomes de funcoes historicos

`criar_dataset_complicacao` tambem e usada pela internacao. Isso reduz clareza. Renomear cedo demais pode quebrar muita coisa; melhor extrair servicos primeiro e renomear depois.

### Graficos e analise dependem das colunas atuais

As fases 2 e 3 esperam colunas como `STATUS CHAVE`, `PROCESSO`, `ACAO`, `QT TELEFONES`. A refatoracao pode mudar a origem desses valores, mas nao deve mudar os nomes de saida no primeiro ciclo.

## Plano recomendado de primeira refatoracao

Primeiro ciclo pequeno:

1. Criar `chave_orquestracao_service.py`.
2. Implementar somente normalizacao e extracao de senha da chave.
3. Adicionar testes para chave normal, chave com senha e chave vazia.
4. Usar esse servico dentro de `_montar_df_final_complicacao` ou no inicio de `_enriquecer_dataset_com_status`.
5. Manter as colunas de saida iguais.

Segundo ciclo:

1. Extrair match principal/fallback para o servico de chave.
2. Criar testes para `OK_PRINCIPAL`, `OK_FALLBACK` e `SEM_MATCH`.
3. Manter resultado final identico.

Terceiro ciclo:

1. Extrair telefones para `telefone_orquestracao_service.py`.
2. Testar prioridade, proximo telefone e historico `TELEFONE STATUS 1..5`.

Quarto ciclo:

1. Aproximar contagens e classificacao.
2. Criar `decisao_orquestracao_service.py`.
3. Fazer `aplicar_classificacao_processo_acao` delegar para esse servico.

## Resumo executivo

Comece por `src/services/dataset_service.py`, mas nao editando tudo direto. Primeiro extraia a chave para um servico novo e cubra com teste.

A ordem de refatoracao mais segura e:

1. chave;
2. match;
3. telefone/numeracao;
4. contagens;
5. classificacao;
6. graficos/analise;
7. separacao definitiva complicacao/internacao.

O objetivo da primeira etapa nao deve ser reduzir codigo imediatamente. Deve ser criar uma camada clara chamada "chave da orquestracao". Depois disso, a reducao de codigo vem naturalmente.
