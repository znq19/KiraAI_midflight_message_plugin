from dataclasses import dataclass, field
from typing import Any


@dataclass
class OpenAIMessage:
    role: str = "user"
    content: Any = ""
    reasoning_content: str | None = None
    tool_calls: list | None = None
    tool_call_id: str | None = None
    name: str | None = None
    extra: dict = field(default_factory=dict)

    def to_dict(self):
        d = {"role": self.role, "content": self.content}
        if self.reasoning_content:
            d["reasoning_content"] = self.reasoning_content
        if self.tool_calls:
            d["tool_calls"] = self.tool_calls
        if self.tool_call_id:
            d["tool_call_id"] = self.tool_call_id
        if self.name:
            d["name"] = self.name
        return d
