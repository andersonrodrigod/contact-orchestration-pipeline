# README - Sistematizacao de Metricas do Pipeline

## Contexto
O pipeline atual ja realiza corretamente a migracao de dados para planilhas.  
Este documento define a padronizacao das metricas em cada etapa para permitir analise visual, geracao de graficos e montagem de dashboards/relatorios.

## Objetivo
- Introduzir metricas estruturadas em todas as fases do pipeline.
- Garantir rastreabilidade dos dados de entrada, transformacao e saida.
- Gerar um JSON unico e padronizado para consumo posterior.
- Usar esse JSON como fonte para:
  - graficos (Matplotlib ou Seaborn)
  - exportacao de imagens (1 imagem por grafico)
  - insercao em slides

---

## Fase 1 - Join de `status` com `status_resposta`

### Regra 1 - Metricas de correspondencia (join)
Calcular:
- Quantidade total de linhas do arquivo `Status`
- Quantidade total de linhas do arquivo `Status_Resposta`
- Quantidade total de registros com correspondencia no join
- Quantidade total de registros sem correspondencia

### Regra 2 - Metricas apos o join
Calcular:
- Quantidade total de cada valor da coluna `Status`
- Quantidade total de cada valor da coluna `Status` agrupado por `DT ENVIO`
- Quantidade total de registros onde `Status = "Lida"`
- Quantidade total de registros onde `Status = "Lida"` agrupado por `DT ENVIO`

### Metricas de resposta (somente `Status = "Lida"`)
Filtrar:
- Registros com `Status = "Lida"`

Calcular:
- Quantidade de cada valor da coluna `RESPOSTA`:
  - `Sim`
  - `Nao`
  - `Sem resposta`
  - `Nao tenho interesse`

Tambem calcular as mesmas metricas agrupadas por `DT ENVIO`.

---

## Fase 2 - Envio de Status

### Colunas QT consideradas
- `QT_LIDA_RESPOSTA_SIM`
- `QT_LIDA_RESPOSTA_NAO`
- `QT_LIDA_SEM_RESPOSTA`
- `QT_LIDA`
- `QT_ENTREGUE`
- `QT_ENVIADA`
- `QT_NAO_ENTREGUE_META`
- `QT_MENSAGEM_NAO_ENTREGUE`
- `QT_EXPERIMENTO`
- `QT_OPT_OUT`

### Regras de metricas

#### 1) Soma por linha
Para cada linha, calcular:
- `soma_qt_sem_lida`: soma de todas as colunas QT, exceto `QT_LIDA`
- `soma_qt_sem_respostas_lida`: soma de todas as colunas QT, exceto:
  - `QT_LIDA_RESPOSTA_SIM`
  - `QT_LIDA_RESPOSTA_NAO`
  - `QT_LIDA_SEM_RESPOSTA`

#### 2) Soma por coluna
Calcular:
- Soma total de cada coluna QT (excluindo `QT_LIDA`)
- Soma total de cada coluna QT (excluindo as colunas de resposta lida):
  - `QT_LIDA_RESPOSTA_SIM`
  - `QT_LIDA_RESPOSTA_NAO`
  - `QT_LIDA_SEM_RESPOSTA`

#### 3) Contagem de registros validos por coluna QT
Para cada coluna QT:
- Contar quantas linhas possuem valor valido
- Considerar apenas valores nao nulos e nao vazios

#### 4) Coluna adicional `QT_TELEFONES`
Calcular:
- Soma total dos valores da coluna
- Quantidade de ocorrencias de cada valor observado

---

## Fase 3 - Orquestracao

### Metricas obrigatorias
Calcular:
- Quantidade total de cada valor da coluna `PROCESSO` (ignorar nulos/vazios)
- Quantidade total de cada valor da coluna `STATUS_CHAVE`

### Classificacao de acoes
Contar registros onde:
- `ACAO = ENCERRADO`
- `ACAO = SEM TELEFONE`
- `ACAO = SEM_TELEFONE_DISPONIVEL`

Para os demais valores de `ACAO`:
- Classificar como `EM_ANDAMENTO`

Para valores vazios de `ACAO`:
- Classificar como `PROGRAMADO`

---

## Orquestracao diaria (segunda camada)
Para orquestracao, as metricas devem ser persistidas em uma camada de historico separada do arquivo final (snapshot mais recente).

Regras:
- `DT_ENVIO` define a data da metrica diaria.
- O snapshot final continua existindo como hoje (sem quebra de compatibilidade).
- A camada de historico recebe apenas dados de orquestracao.
- Duplicacao de reexecucao deve ser controlada por hash de conteudo.

Regra de idempotencia:
- Mesmo `DT_ENVIO` + mesmo hash: descartar (execucao duplicada).
- Mesmo `DT_ENVIO` + hash diferente: considerar atualizacao valida.
- Para evitar inflar contagem diaria, considerar a ultima versao da chave de negocio no dia.

Exemplo de chave de negocio:
- `CONTATO + DT_ENVIO`

Exemplo de hash de conteudo:
- hash de (`PROCESSO`, `STATUS_CHAVE`, `ACAO`)

---

## JSON reduzido (somente orquestracao)
Exemplo com 3 dias seguidos:

```json
{
  "metadata": {
    "pipeline": "complicacao_orquestracao_pipeline",
    "versao_metricas": "1.0.0",
    "competencia": "2026-03"
  },
  "orquestracao_por_dt_envio": {
    "2026-03-01": {
      "processo": {
        "EM_ANDAMENTO": 120,
        "ENCERRADO": 80,
        "PROGRAMADO": 15
      },
      "status_chave": {
        "CHAVE_OK": 170,
        "CHAVE_PENDENTE": 45
      },
      "classificacao_acao": {
        "ENCERRADO": 72,
        "SEM TELEFONE": 9,
        "SEM_TELEFONE_DISPONIVEL": 4,
        "EM_ANDAMENTO": 110,
        "PROGRAMADO": 20
      }
    },
    "2026-03-02": {
      "processo": {
        "EM_ANDAMENTO": 118,
        "ENCERRADO": 84,
        "PROGRAMADO": 12
      },
      "status_chave": {
        "CHAVE_OK": 168,
        "CHAVE_PENDENTE": 46
      },
      "classificacao_acao": {
        "ENCERRADO": 76,
        "SEM TELEFONE": 7,
        "SEM_TELEFONE_DISPONIVEL": 5,
        "EM_ANDAMENTO": 106,
        "PROGRAMADO": 20
      }
    },
    "2026-03-03": {
      "processo": {
        "EM_ANDAMENTO": 125,
        "ENCERRADO": 79,
        "PROGRAMADO": 14
      },
      "status_chave": {
        "CHAVE_OK": 172,
        "CHAVE_PENDENTE": 46
      },
      "classificacao_acao": {
        "ENCERRADO": 70,
        "SEM TELEFONE": 10,
        "SEM_TELEFONE_DISPONIVEL": 6,
        "EM_ANDAMENTO": 112,
        "PROGRAMADO": 20
      }
    }
  }
}
```

---

## Diretriz de visualizacao (graficos e slides)

### Fonte de dados
- Ler exclusivamente o JSON de metricas consolidado.

### Graficos recomendados
- Barras: contagem por `Status`
- Barras empilhadas ou heatmap: `Status` por `DT ENVIO`
- Pizza ou barras: distribuicao de `RESPOSTA` em `Status = Lida`
- Barras: somas por coluna QT
- Histograma ou barras: ocorrencias de `QT_TELEFONES`
- Barras: distribuicao por `PROCESSO`, `STATUS_CHAVE` e classificacao de `ACAO` por `DT_ENVIO`

### Exportacao e apresentacao
- Gerar 1 arquivo de imagem por grafico (`.png`)
- Nomear arquivos com prefixo da fase e metrica
- Inserir cada imagem em um slide dedicado

Exemplo de padrao de nome:
- `fase1_status_por_dt_envio.png`
- `fase2_soma_colunas_qt.png`
- `fase3_classificacao_acao.png`

---

## Criterios de qualidade
- Todas as metricas devem ser reproduziveis e deterministicas.
- Evitar dupla contagem (validar chaves de agregacao).
- Registrar no log quando filtros forem aplicados (ex.: `Status = Lida`).
- Garantir consistencia de nomenclatura (`DT ENVIO`, `Status`, `RESPOSTA`, `ACAO`).
- Em campos textuais, padronizar variacoes (`Nao` vs `Não`) antes da agregacao.

## Resultado esperado
- Um JSON estruturado de orquestracao com metricas diarias por `DT_ENVIO`.
- Um conjunto de graficos gerados a partir desse JSON diario.
- Um slide por grafico, pronto para apresentacao e analise.
