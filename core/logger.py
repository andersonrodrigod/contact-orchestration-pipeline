from __future__ import annotations

from datetime import datetime
from pathlib import Path
import traceback


class PipelineLogger:
    def __init__(self, nome_pipeline='ingestao_pipeline', pasta_logs='logs'):
        self.nome_pipeline = nome_pipeline
        self.inicio = datetime.now()
        self._ultima_etapa = None

        pasta = Path(pasta_logs)
        pasta.mkdir(parents=True, exist_ok=True)

        timestamp = self.inicio.strftime('%Y%m%d_%H%M%S')
        self.caminho_arquivo = pasta / f'{nome_pipeline}_{timestamp}.txt'

        with self.caminho_arquivo.open('a', encoding='utf-8') as arquivo:
            arquivo.write('[EXECUCAO] Inicio do relatorio de execucao\n\n')

        self._escrever_linha('INFO', 'LOGGER', 'Execucao iniciada')

    def _escrever_linha(self, nivel, etapa, mensagem):
        agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        linha = f'[{agora}] [{nivel}] [{etapa}] {mensagem}\n'
        with self.caminho_arquivo.open('a', encoding='utf-8') as arquivo:
            if self._ultima_etapa is not None and etapa != self._ultima_etapa:
                arquivo.write('\n')
            arquivo.write(linha)
        self._ultima_etapa = etapa

    def info(self, etapa, mensagem):
        self._escrever_linha('INFO', etapa, mensagem)

    def warning(self, etapa, mensagem):
        self._escrever_linha('WARN', etapa, mensagem)

    def error(self, etapa, mensagem):
        self._escrever_linha('ERROR', etapa, mensagem)

    def exception(self, etapa, erro):
        self._escrever_linha('ERROR', etapa, f'Excecao: {erro}')
        trilha = traceback.format_exc()
        for linha in trilha.strip().splitlines():
            self._escrever_linha('ERROR', etapa, linha)

    def finalizar(self, status):
        fim = datetime.now()
        duracao = fim - self.inicio
        self._escrever_linha(
            'INFO',
            'LOGGER',
            f'Execucao finalizada com status={status} em {duracao}',
        )
