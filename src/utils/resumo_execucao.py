def imprimir_resumo_execucao(resultado):
    if not isinstance(resultado, dict):
        print('Resultado da execucao indisponivel.')
        return

    print(f"OK: {resultado.get('ok', False)}")
    if 'arquivo_saida' in resultado:
        print(f"Arquivo final: {resultado['arquivo_saida']}")
    if 'total_status' in resultado:
        print(f"Total status: {resultado['total_status']}")
    if 'total_linhas' in resultado:
        print(f"Total dataset: {resultado['total_linhas']}")
    if 'com_match' in resultado:
        print(f"Com match: {resultado['com_match']}")
    if 'sem_match' in resultado:
        print(f"Sem match: {resultado['sem_match']}")
    if 'resultados' in resultado:
        for nome, res in resultado['resultados'].items():
            print(f"{nome}: OK={res.get('ok', False)} arquivo={res.get('arquivo_saida', '')}")
            mensagens_filho = res.get('mensagens', [])
            if mensagens_filho:
                for mensagem in mensagens_filho:
                    print(f"  - {nome}: {mensagem}")
    mensagens = resultado.get('mensagens', [])
    if mensagens:
        print('Mensagens:')
        for mensagem in mensagens:
            print(f'- {mensagem}')
