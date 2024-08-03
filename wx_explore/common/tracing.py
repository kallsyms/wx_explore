def init_tracing(service_name):
    pass


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
    return NoOpSpan()
