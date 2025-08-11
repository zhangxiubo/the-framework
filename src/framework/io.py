import logging
import random
from os import PathLike
from typing import Optional, Union

from pydantic import BaseModel

from .core import AbstractProcessor, Event

logger = logging.getLogger(__name__)


class JsonLLoader(AbstractProcessor):
    def __init__(self, name: str, filepath: str, item_type, attribute: Optional[str] = None):
        super().__init__()
        self.name = name
        self.filepath = filepath
        self.file = open(self.filepath, "rt")
        from pydantic import BaseModel

        assert issubclass(item_type, BaseModel)
        self.item_type = item_type
        self.attribute = name if attribute is None else attribute

    class LoadJsonL:
        def __init__(self, name, limit=None, sample=1.0):
            self.name = name
            self.limit = float("inf") if limit is None else int(limit)
            self.sample = sample

    def process(self, context, event):
        match event:
            case self.LoadJsonL(name=self.name) as e:
                this_logger = logger.getChild(f"{self.__class__.__name__}.{event.name}")
                this_logger.debug(
                    f"loading {self.item_type} from jsonl file: {self.filepath}; limit: {e.limit}, sample: {e.sample}"
                )
                r = random.Random()
                line_count = 0
                loaded_count = 0
                for line in self.file:
                    line_count += 1
                    try:
                        if r.random() < e.sample:
                            context.submit(
                                Event(
                                    name=self.name,
                                    **{
                                        self.attribute: self.item_type.model_validate_json(line)
                                    },
                                )
                            )
                            loaded_count += 1
                            if loaded_count >= e.limit:
                                break
                    except Exception as e:
                        raise e
                this_logger.debug(
                    f"finished loading jsonl... lines loaded {loaded_count} / {line_count} (ratio: {(loaded_count * 100 / line_count) if line_count > 0 else 0:.2f}%)"
                )
                context.submit(
                    Event(
                        name=self.name + "$",
                    )
                )
            case Event(name="__POISON__"):
                self.file.close()


class JsonLWriter(AbstractProcessor):
    def __init__(self, name, filepath: Union[PathLike, str], item_type, attribute: Optional[str] = None):
        super().__init__()
        self.name = name
        self.filepath = filepath
        assert issubclass(item_type, BaseModel)
        self.item_type = item_type
        self.attribute = name if attribute is None else attribute
        self.file = open(self.filepath, "wt")

    def process(self, context, event):
        match event:
            case (Event(name=self.name) as e) if isinstance(getattr(e, self.attribute, None), self.item_type):
                payload = getattr(e, self.attribute)
                self.file.writelines((payload.model_dump_json(), "\n"))
                self.file.flush()
            case Event(name="__POISON__"):
                self.file.close()
