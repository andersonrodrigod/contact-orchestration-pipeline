import json
from datetime import datetime
from pathlib import Path


ARQUIVO_HISTORICO_EXECUCOES = Path('logs/historico_execucoes.jsonl')


def _to_bool(valor):
    return bool(valor)


def _coletar_metricas(resultado):
    if not isinstance(resultado, dict):
        return {}
    chaves_fixas = [
        'total_status',
        'total_linhas',
        'com_match',
        'sem_match',
        'linhas_status',
        'linhas_status_resposta',
        'linhas_dataset_origem',
        'descartados_status_data_invalida',
        'descartados_resposta_data_invalida',
        'limiar_nat_data_em_uso',
    ]
    metricas = {}
    for chave in chaves_fixas:
        if chave in resultado:
            metricas[chave] = resultado.get(chave)
    for chave, valor in resultado.items():
        if not isinstance(chave, str):
            continue
        if chave.startswith('pct_nat_') or chave.startswith('nat_'):
            metricas[chave] = valor
    return metricas


def _coletar_qualidade_data(resultado):
    if not isinstance(resultado, dict):
        return None

    if isinstance(resultado.get('qualidade_data'), dict):
        return resultado.get('qualidade_data')

    detalhes_raiz = resultado.get('detalhes')
    if isinstance(detalhes_raiz, dict):
        qualidade = detalhes_raiz.get('qualidade_data')
        if isinstance(qualidade, dict):
            return qualidade

    dados = resultado.get('dados')
    if not isinstance(dados, dict):
        return None

    detalhes = dados.get('detalhes')
    if not isinstance(detalhes, dict):
        return None

    qualidade = detalhes.get('qualidade_data')
    if isinstance(qualidade, dict):
        return qualidade
    return None


def _coletar_metricas_por_etapa(resultado):
    if not isinstance(resultado, dict):
        return None

    metricas_por_etapa = resultado.get('metricas_por_etapa')
    if isinstance(metricas_por_etapa, dict):
        return metricas_por_etapa

    detalhes_raiz = resultado.get('detalhes')
    if isinstance(detalhes_raiz, dict):
        metricas_por_etapa = detalhes_raiz.get('metricas_por_etapa')
        if isinstance(metricas_por_etapa, dict):
            return metricas_por_etapa

    dados = resultado.get('dados')
    if not isinstance(dados, dict):
        return None

    metricas_por_etapa = dados.get('metricas_por_etapa')
    if isinstance(metricas_por_etapa, dict):
        return metricas_por_etapa

    detalhes = dados.get('detalhes')
    if not isinstance(detalhes, dict):
        return None
    metricas_por_etapa = detalhes.get('metricas_por_etapa')
    if isinstance(metricas_por_etapa, dict):
        return metricas_por_etapa
    return None


def registrar_historico_execucao(resultado, modo, arquivo_historico=ARQUIVO_HISTORICO_EXECUCOES):
    if not isinstance(resultado, dict):
        return None

    caminho = Path(arquivo_historico)
    caminho.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        'timestamp': datetime.now().isoformat(),
        'modo': modo,
        'ok': _to_bool(resultado.get('ok')),
        'codigo_erro': resultado.get('codigo_erro'),
        'metricas': _coletar_metricas(resultado),
    }
    qualidade_data = _coletar_qualidade_data(resultado)
    if qualidade_data is not None:
        payload['qualidade_data'] = qualidade_data
    metricas_por_etapa = _coletar_metricas_por_etapa(resultado)
    if metricas_por_etapa is not None:
        payload['metricas_por_etapa'] = metricas_por_etapa

    resultados_filho = resultado.get('resultados')
    if isinstance(resultados_filho, dict):
        payload['resultados'] = {
            nome: {
                'ok': _to_bool(res.get('ok')),
                'codigo_erro': res.get('codigo_erro'),
                'metricas': _coletar_metricas(res),
                'qualidade_data': _coletar_qualidade_data(res),
                'metricas_por_etapa': _coletar_metricas_por_etapa(res),
            }
            for nome, res in resultados_filho.items()
            if isinstance(res, dict)
        }

    with caminho.open('a', encoding='utf-8') as arquivo:
        arquivo.write(json.dumps(payload, ensure_ascii=False) + '\n')
    return str(caminho)
