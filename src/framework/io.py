"""Streaming IO processors bridging external JSON Lines (JSONL) data and
the internal event-driven runtime with typed payloads.

This module provides two processors that integrate file IO with the framework's
event pipeline:

- Loader (JsonLLoader):
  Receives LoadJsonL(name, limit, sample) → reads lines from a JSONL file →
  emits Event(name=name, attribute=item) for each accepted line (typed via Pydantic)
  → finally emits Event(name=name + "$") as a completion sentinel.

- Writer (JsonLWriter):
  Listens for Event(name=name, attribute=item_type) → writes each payload as a
  JSON Line → flushes the file; closes on the __POISON__ event.

Execution model:
Each processor inherits from AbstractProcessor and therefore runs on a
single-threaded executor per instance. This serializes state changes and ensures
file access is not interleaved within a single processor, simplifying resource
management.

Sampling and limits:
Sampling is probabilistic per line using random.Random(). The 'limit' parameter
is enforced on the number of emitted items (accepted lines), not on the number of
raw lines read from the file.

Notes:
- The completion sentinel uses the convention name + "$" to indicate end-of-stream.
- Resources (open files) are closed when a "__POISON__" Event is received.

"""
import logging
import random
from os import PathLike
from typing import Optional, Union

from pydantic import BaseModel

from .core import AbstractProcessor, Event

logger = logging.getLogger(__name__)


class JsonLLoader(AbstractProcessor):
    """Stream a JSONL file into the event pipeline as typed items.

    - Purpose: Bridge external JSONL data into the framework as Pydantic-typed
      payloads carried by Event instances.
    - Lifecycle: Open the file in text mode on construction and keep it open
      until a "__POISON__" Event triggers resource cleanup.

    __init__ parameters:
    - name: Logical stream name. Also the default attribute key for emitted payloads.
    - filepath: Path to the JSONL file to read.
    - item_type: A subclass of pydantic.BaseModel used to validate each JSON line.
    - attribute: Optional attribute name under which payloads are attached to Events.
                 Defaults to 'name' if not provided.

    Validation and side effects:
    - Validates that 'item_type' is a subclass of BaseModel.
    - Opens the file in text mode ('rt') immediately.

    Resource management:
    - The open file handle is closed when a "__POISON__" Event is processed.
    """

    def __init__(self, name: str, filepath: str, item_type, attribute: Optional[str] = None):
        """Initialize the loader and open the JSONL file.

        Parameters:
        - name: Stream name used for emitted events and as default payload attribute.
        - filepath: Path to the source JSONL file.
        - item_type: Expected Pydantic model type for each item (must be a subclass of BaseModel).
        - attribute: Optional attribute name for emitted payloads; defaults to 'name' when None.

        Behavior:
        - Asserts 'item_type' is a subclass of BaseModel.
        - Opens 'filepath' for reading in text mode.
        - Sets 'self.attribute' to the provided value or falls back to 'name'.

        Resource lifecycle:
        - The file is closed when a "__POISON__" Event is processed.
        """
        super().__init__()
        self.name = name
        self.filepath = filepath
        self.file = open(self.filepath, "rt")
        from pydantic import BaseModel

        assert issubclass(item_type, BaseModel)
        self.item_type = item_type
        self.attribute = name if attribute is None else attribute

    class LoadJsonL:
        """Command-envelope payload for triggering file reads.

        Parameters:
        - name: Target stream name; must match the processor's 'name' to take effect.
        - limit: Maximum number of items to emit. None means no limit (treated as infinity).
        - sample: Probability in [0.0, 1.0] used to accept each line independently.

        Semantics:
        - Sampling is applied per line using a fresh random.Random() instance.
        - The 'limit' is enforced on the number of emitted items, not lines read.
        """

        def __init__(self, name, limit=None, sample=1.0):
            self.name = name
            self.limit = float("inf") if limit is None else int(limit)
            self.sample = sample

    def process(self, context, event):
        """Handle events for loading or cleanup.

        Matches:
        - LoadJsonL(name=self.name):
          Reads the file line by line, applies probabilistic sampling, validates
          each accepted line as 'item_type', and emits Event(name=self.name, attribute=payload)
          for each item. After processing, emits a completion sentinel Event with
          name '<name>$'. Logging records counts and ratios.

        - Event(name="__POISON__"):
          Closes the open file handle to release OS resources.

        Error handling:
        - Any exception during parsing/validation is re-raised for the runtime to handle.
          No custom suppression or logging occurs here.
        """
        match event:
            case self.LoadJsonL(name=self.name) as e:
                this_logger = logger.getChild(f"{self.__class__.__name__}.{event.name}")
                this_logger.debug(
                    f"loading {self.item_type} from jsonl file: {self.filepath}; limit: {e.limit}, sample: {e.sample}"
                )
                r = random.Random()
                # Use an RNG instance for per-line independent sampling decisions.
                # TODO: Consider parameterizing the RNG for deterministic tests or seeding from context.
                line_count = 0
                loaded_count = 0
                for line in self.file:
                    line_count += 1
                    try:
                        # Probabilistic acceptance: only a fraction of lines are emitted based on 'sample'.
                        # Note: 'loaded_count' tracks emitted items (accepted lines), not raw lines read.
                        if r.random() < e.sample:
                            # Emit a typed Event. The payload attribute name defaults to the processor
                            # 'name' unless overridden by 'attribute' at construction.
                            context.submit(
                                Event(
                                    name=self.name,
                                    **{
                                        self.attribute: self.item_type.model_validate_json(line)
                                    },
                                )
                            )
                            loaded_count += 1
                            # Enforce the item-emission limit; continue reading until reaching the limit or EOF.
                            if loaded_count >= e.limit:
                                break
                    except Exception as e:
                        # Propagate parsing/validation errors; no custom handling here.
                        # TODO: Consider explicit error handling/log levels for malformed JSON lines (currently re-raises).
                        raise e
                this_logger.debug(
                    f"finished loading jsonl... lines loaded {loaded_count} / {line_count} (ratio: {(loaded_count * 100 / line_count) if line_count > 0 else 0:.2f}%)"
                )
                # Emit completion sentinel signaling end-of-stream using the "<name>$" convention.
                context.submit(
                    Event(
                        name=self.name + "$",
                    )
                )
                # TODO: Consider backpressure or flow-control mechanisms when emitting large volumes of events.
            case Event(name="__POISON__"):
                # Close the file to release OS resources; additional cleanup should also live here if added later.
                self.file.close()


class JsonLWriter(AbstractProcessor):
    """Write typed events to a JSONL file.

    - Purpose: Persist Pydantic-typed payloads carried by Events to a JSONL file.
    - Lifecycle: Open the file for writing on construction; close on "__POISON__".

    __init__ parameters:
    - name: Stream name to listen for.
    - filepath: Destination path for the JSONL output.
    - item_type: Pydantic model type expected on incoming Events.
    - attribute: Optional attribute name to read payloads from; defaults to 'name'.

    Validation and side effects:
    - Validates that 'item_type' is a subclass of BaseModel.
    - Opens the file in text mode for writing ('wt') immediately.
    """

    def __init__(self, name, filepath: Union[PathLike, str], item_type, attribute: Optional[str] = None):
        """Initialize the writer and open the destination JSONL file.

        Parameters:
        - name: Stream name the writer subscribes to.
        - filepath: Output path for JSON lines.
        - item_type: Expected Pydantic model type to be written (subclass of BaseModel).
        - attribute: Attribute name on Event holding the payload; defaults to 'name' if None.

        Behavior:
        - Asserts 'item_type' is a subclass of BaseModel.
        - Opens 'filepath' for writing in text mode.
        - Sets 'self.attribute' accordingly.
        """
        super().__init__()
        self.name = name
        self.filepath = filepath
        assert issubclass(item_type, BaseModel)
        self.item_type = item_type
        self.attribute = name if attribute is None else attribute
        self.file = open(self.filepath, "wt")

    def process(self, context, event):
        """Handle events by writing typed payloads or performing cleanup.

        Matches:
        - Event(name=self.name) with a payload attribute that is an instance of 'item_type':
          Writes the payload as a JSON line using model_dump_json(), then flushes the file
          to ensure durability.

        - Event(name="__POISON__"):
          Closes the open file handle to release OS resources.

        Typed guard:
        - Uses isinstance(getattr(e, self.attribute, None), self.item_type) to ensure the
          attribute exists and is of the expected type before writing.

        Flush strategy:
        - Flushes after each write. This favors durability and simplicity over throughput.
          Consider batching or configurable flush policies for high-volume scenarios.
        """
        match event:
            # Guard: only write when the configured attribute exists and matches the expected Pydantic type.
            case (Event(name=self.name) as e) if isinstance(getattr(e, self.attribute, None), self.item_type):
                payload = getattr(e, self.attribute)
                self.file.writelines((payload.model_dump_json(), "\n"))
                # Flush after each write for durability; this can reduce throughput for large streams.
                # TODO: Consider batching writes and a configurable flush policy in the writer for performance.
                self.file.flush()
            case Event(name="__POISON__"):
                # Close the file to release OS resources; additional cleanup should also live here if added later.
                self.file.close()
