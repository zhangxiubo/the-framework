
from framework import Pipeline
from framework import AbstractProcessor
from framework import Event

class TestProcessor(AbstractProcessor):

    def process(self, context, event):
        match event:
            case Event(name='test', payload=payload):
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