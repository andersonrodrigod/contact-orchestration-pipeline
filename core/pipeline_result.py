from dataclasses import dataclass, field


@dataclass
class PipelineResult:
    ok: bool
    mensagens: list[str] = field(default_factory=list)
    metricas: dict = field(default_factory=dict)
    arquivos: dict = field(default_factory=dict)
    dados: dict = field(default_factory=dict)
    codigo_erro: str | None = None

    def to_dict(self):
        resultado = {'ok': self.ok, 'mensagens': self.mensagens}
        if self.codigo_erro:
            resultado['codigo_erro'] = self.codigo_erro
        resultado.update(self.metricas)
        resultado.update(self.arquivos)
        resultado.update(self.dados)
        return resultado


def ok_result(mensagens=None, metricas=None, arquivos=None, dados=None, codigo_erro=None):
    return PipelineResult(
        ok=True,
        mensagens=mensagens or [],
        metricas=metricas or {},
        arquivos=arquivos or {},
        dados=dados or {},
        codigo_erro=codigo_erro,
    ).to_dict()


def error_result(mensagens=None, metricas=None, arquivos=None, dados=None, codigo_erro=None):
    return PipelineResult(
        ok=False,
        mensagens=mensagens or [],
        metricas=metricas or {},
        arquivos=arquivos or {},
        dados=dados or {},
        codigo_erro=codigo_erro,
    ).to_dict()
