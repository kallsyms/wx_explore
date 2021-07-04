from opentelemetry import trace
from opentelemetry.ext.honeycomb import HoneycombSpanExporter
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


tracer = None


def init_tracing(service_name):
    from wx_explore.common.config import Config

    if Config.TRACE_EXPORTER is None:
        return
    elif Config.TRACE_EXPORTER == 'jaeger':
        exporter = JaegerExporter(
            agent_host_name=Config.JAEGER_HOST,
            agent_port=6831,
        )
    elif Config.TRACE_EXPORTER == 'honeycomb':
        exporter = HoneycombSpanExporter(
            service_name=service_name,
            writekey=Config.HONEYCOMB_API_KEY,
            dataset=Config.HONEYCOMB_DATASET,
        )
    else:
        raise ValueError(f"TRACE_EXPORTER {Config.TRACE_EXPORTER} is not valid")

    resource = Resource.create(attributes={"service.name": service_name})

    trace.set_tracer_provider(TracerProvider(resource=resource))
    span_processor = BatchSpanProcessor(exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    # This isn't great but oh well
    global tracer
    tracer = trace.get_tracer(service_name)


class NoOpSpan():  #(trace.Span)
    """
    Dummy Span class for use when no tracing is desired
    """
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def set_attribute(self, *args, **kwargs):
        pass


def start_span(span_name, parent=None):
    if tracer:
        return tracer.start_as_current_span(span_name)
    else:
        return NoOpSpan()
