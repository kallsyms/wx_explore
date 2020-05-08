from opentelemetry import trace
from opentelemetry.ext.honeycomb import HoneycombSpanExporter
from opentelemetry.ext.jaeger import JaegerSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor

from wx_explore.common.config import Config


tracer = None


def init_tracing(service_name):
    if Config.TRACE_EXPORTER is None:
        return
    elif Config.TRACE_EXPORTER == 'jaeger':
        exporter = JaegerSpanExporter(
            service_name=service_name,
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

    trace.set_tracer_provider(TracerProvider())
    span_processor = BatchExportSpanProcessor(exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    # This isn't great but oh well
    global tracer
    tracer = trace.get_tracer(service_name)


def start_span(span_name):
    return tracer.start_as_current_span(span_name)
