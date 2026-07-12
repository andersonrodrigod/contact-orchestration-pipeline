import json
from datetime import datetime
from pathlib import Path


ARQUIVO_HISTORICO_EXECUCOES = Path('logs/historico_execucoes.jsonl')


def _to_bool(valor):
    return bool(valor)


def _identificador_trimestre(data_referencia):
    trimestre = ((data_referencia.month - 1) // 3) + 1
    return f'{data_referencia.year}_Q{trimestre}'


def _resolver_arquivo_historico_trimestral(arquivo_historico, data_referencia):
    caminho = Path(arquivo_historico)
    identificador = _identificador_trimestre(data_referencia)
    nome_rotacionado = f'{caminho.stem}_{identificador}{caminho.suffix}'
    return caminho.with_name(nome_rotacionado)


def registrar_historico_execucao(resultado, modo, arquivo_historico=ARQUIVO_HISTORICO_EXECUCOES):
    if not isinstance(resultado, dict):
        return None

    timestamp_execucao = datetime.now()
    caminho = _resolver_arquivo_historico_trimestral(arquivo_historico, timestamp_execucao)
    caminho.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        'timestamp': timestamp_execucao.isoformat(),
        'modo': modo,
        'ok': _to_bool(resultado.get('ok')),
        'codigo_erro': resultado.get('codigo_erro'),
    }

    resultados_filho = resultado.get('resultados')
    if isinstance(resultados_filho, dict):
        payload['resultados'] = {
            nome: {
                'ok': _to_bool(res.get('ok')),
                'codigo_erro': res.get('codigo_erro'),
            }
            for nome, res in resultados_filho.items()
            if isinstance(res, dict)
        }

    with caminho.open('a', encoding='utf-8') as arquivo:
        arquivo.write(json.dumps(payload, ensure_ascii=False) + '\n')
    return str(caminho)
