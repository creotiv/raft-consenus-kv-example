from __future__ import annotations
import logging
from typing import Any, Dict, Iterable, Optional, TypeVar, Generic
from pydantic import BaseModel

logger = logging.getLogger("STATE UPDATE")

T = TypeVar("T", bound=BaseModel)


def _is_pydantic_v2(model: BaseModel) -> bool:
    return hasattr(model, "model_dump")


def _model_copy(
    model: BaseModel, *, update: Optional[Dict[str, Any]] = None
) -> BaseModel:
    """Compatibility: Pydantic v1 uses .copy, v2 uses .model_copy."""
    if _is_pydantic_v2(model):
        return model.model_copy(update=update or {})
    return model.copy(update=update or {})  # type: ignore[attr-defined]


def _model_dump(model: BaseModel) -> Dict[str, Any]:
    """Compatibility: Pydantic v1 uses .dict, v2 uses .model_dump."""
    if _is_pydantic_v2(model):
        return model.model_dump()
    return model.dict()  # type: ignore[attr-defined]


def _model_fields(model: BaseModel) -> Iterable[str]:
    """Return field names for v1/v2."""
    if _is_pydantic_v2(model):
        return model.model_fields.keys()  # type: ignore[attr-defined]
    return model.__fields__.keys()  # type: ignore[attr-defined]


def _format_list_summary(value: Iterable[Any]) -> str:
    seq = list(value)
    if not seq:
        return "[]"
    if len(seq) == 1:
        return f"[{seq[0]!r}]"
    return f"[{seq[0]!r}..{seq[-1]!r}]"


def _format_value(v: Any) -> str:
    # Lists/tuples: show [first..last]
    if isinstance(v, (list, tuple)):
        return _format_list_summary(v)
    # Nested Pydantic model: compact dict
    if isinstance(v, BaseModel):
        return repr(_model_dump(v))
    # Dict: show shallow keys
    if isinstance(v, dict):
        # limit size a bit
        keys = list(v.keys())
        if len(keys) <= 6:
            return repr(v)
        return f"{{{', '.join(map(repr, keys[:5]))}, ...}}"
    # Fallback
    return repr(v)


class StateController(Generic[T]):
    """
    Wraps a Pydantic model and logs every state transition.
    - update(new_data): state = state.copy(update=new_data)
    - set(field, value): convenience setter with logging
    - replace(new_state): replace whole model (logs diff of provided fields)
    - snapshot(): return a copy
    """

    def __init__(self, state: T):
        self._state: T = state
        self._logger = logger

    @property
    def state(self) -> T:
        return self._state

    def snapshot(self) -> T:
        return _model_copy(self._state)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            self.set(name, value)

    def isStateMutated(self, **kwargs) -> bool:
        """
        State guard.

        Check if the state is mutated by comparing the current state with the expected state.
        """
        for k, v in kwargs.items():
            if getattr(self._state, k) != v:
                logger.error(f"State mutated: {k} {getattr(self._state, k)} -> {v}")
                return True
        return False

    def __getattr__(self, name):
        return self._state.__getattribute__(name)

    def set(self, field: str, value: Any) -> None:
        if field not in _model_fields(self._state):
            raise AttributeError(
                f"Unknown field '{field}' for {type(self._state).__name__}"
            )
        self.update({field: value})

    def append(self, field: str, value: Any) -> None:
        if field not in _model_fields(self._state):
            raise AttributeError(
                f"Unknown field '{field}' for {type(self._state).__name__}"
            )
        before = self._state.__getattribute__(field)
        before[field].append(value)
        self.update({field: before})

    def extend(self, field: str, value: Any) -> None:
        if field not in _model_fields(self._state):
            raise AttributeError(
                f"Unknown field '{field}' for {type(self._state).__name__}"
            )
        before = self._state.__getattribute__(field)
        before.extend(value)
        self.update({field: before})

    def replace(self, new_state: T) -> None:
        # Log differences of fields that changed (optimized)
        before_values = {}
        changed = {}
        for field in _model_fields(new_state):
            current_val = getattr(self._state, field, None)
            new_val = getattr(new_state, field)
            if current_val != new_val:
                before_values[field] = current_val
                changed[field] = new_val

        if changed:
            self._log_change(changed, before_values, changed)
        self._state = new_state

    def update(self, new_data: Dict[str, Any] | BaseModel) -> None:
        """Apply a partial update via state.copy(update=...). Logs per-field prev/new."""
        if isinstance(new_data, BaseModel):
            new_data = _model_dump(new_data)

        # Filter to known fields only (avoid silent extra keys)
        allowed = set(_model_fields(self._state))
        clean_update = {k: v for k, v in new_data.items() if k in allowed}
        if not clean_update:
            return  # nothing to do

        # Get current values for fields being updated (much faster than full dump)
        before_values = {}
        changed = {}
        for field in clean_update.keys():
            current_val = getattr(self._state, field)
            new_val = clean_update[field]
            if current_val != new_val:
                before_values[field] = current_val
                changed[field] = new_val

        if changed:
            # Only create new state if there are actual changes
            new_state = _model_copy(self._state, update=changed)
            self._log_change(changed, before_values, changed)
            self._state = new_state

    # --- Logging ---
    def _log_change(
        self, changed: Dict[str, Any], before: Dict[str, Any], after: Dict[str, Any]
    ) -> None:
        lines = [""]
        for field in changed.keys():
            prev_val = _format_value(before.get(field))
            new_val = _format_value(after.get(field))
            lines.append(f"#    {field}: {prev_val} -> {new_val}")
        msg = "\n".join(lines)
        self._logger.debug(msg)

    def debug(self, msg: str) -> None:
        self._logger.debug(msg)


if __name__ == "__main__":

    class A(BaseModel):
        log: list[int]
        b: int

    logging.basicConfig(level=logging.DEBUG)

    state = A(log=[1, 2, 3], b=1)
    ctrl = StateController(state)
    ctrl.append("log", 4)
    ctrl.log = [12]
    print(ctrl.log)
    ctrl.extend("log", [5, 6, 7])
    ctrl.set("log", [12, 13, 14])
    ctrl.update({"log": [12, 13, 15]})
    ctrl.replace(A(log=[1, 2, 3, 4, 5, 6, 7, 12, 13, 14], b=2))
