from __future__ import annotations

from typing import List

from pydantic import BaseModel

from framework.core import Pipeline, AbstractProcessor
from framework.reactive import ReactiveEvent, JsonLSink


class Item(BaseModel):
    x: int
    y: str


class ReactiveModelSource(AbstractProcessor):
    """
    Source that emits BaseModel artifacts on resolve(target=name).
    """

    def __init__(self, name: str, artifacts: List[BaseModel]):
        super().__init__()
        self.name = name
        self.artifacts = list(artifacts)

    def process(self, context, event):
        match event:
            case ReactiveEvent(name="resolve", target=target) if target == self.name:
                for a in self.artifacts:
                    context.submit(
                        ReactiveEvent(name="built", target=self.name, artifact=a)
                    )


def test_jsonl_sink_writes_one_json_per_line_and_closes(tmp_path):
    """
    JsonLSink behavior:
      - Requires and provides the same topic name (name).
      - On built(name, artifact=BaseModel), writes one JSON per line to the file.
      - File is flushed after each write and closed on termination (__POISON__ during Pipeline.run()).
    """
    items = [Item(x=1, y="a"), Item(x=2, y="b")]
    src = ReactiveModelSource("J", items)
    out_path = tmp_path / "out.jsonl"
    sink = JsonLSink(name="J", filepath=str(out_path))

    p = Pipeline(
        processors=[src, sink],
        strict_interest_inference=False,
        workspace=None,
    )

    # Kick off sink and source by resolving the shared topic
    p.submit(ReactiveEvent(name="resolve", target="J"))
    p.run()

    # Validate file content: exactly one JSON object per line in the same order
    lines = out_path.read_text().strip().splitlines()
    assert len(lines) == len(items)
    parsed = [Item.model_validate_json(l) for l in lines]
    assert parsed == items