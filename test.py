
from framework.core import Pipeline
from framework.core import AbstractProcessor
from framework.core import Event

class TestProcessor(AbstractProcessor):

    def process(self, context, event):
        match event:
            case Event(name='test', payload=payload):
                print(payload)
                pass
            case Event(name='abc', payload=payload):
                print(payload)
                pass




pipeline = Pipeline([
    TestProcessor()
])
pipeline.submit(
    Event(name='test', payload=123)
)
pipeline.submit(
    Event(name='abc', payload=123)
)
pipeline.run()