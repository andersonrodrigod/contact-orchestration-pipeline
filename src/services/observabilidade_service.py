import json
from datetime import datetime
from pathlib import Path


ARQUIVO_HISTORICO_EXECUCOES = Path('logs/historico_execucoes.jsonl')


def _to_bool(valor):
    return bool(valor)


def _coletar_metricas(resultado):
    if not isinstance(resultado, dict):
        return {}
    chaves = [
        'total_status',
        'total_linhas',
        'com_match',
        'sem_match',
        'descartados_status_data_invalida',
        'descartados_resposta_data_invalida',
    ]
    metricas = {}
    for chave in chaves:
        if chave in resultado:
            metricas[chave] = resultado.get(chave)
    return metricas


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

    resultados_filho = resultado.get('resultados')
    if isinstance(resultados_filho, dict):
        payload['resultados'] = {
            nome: {
                'ok': _to_bool(res.get('ok')),
                'codigo_erro': res.get('codigo_erro'),
                'metricas': _coletar_metricas(res),
            }
            for nome, res in resultados_filho.items()
            if isinstance(res, dict)
        }

    with caminho.open('a', encoding='utf-8') as arquivo:
        arquivo.write(json.dumps(payload, ensure_ascii=False) + '\n')
    return str(caminho)
