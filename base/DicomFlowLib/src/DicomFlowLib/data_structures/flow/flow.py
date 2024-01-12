
from typing import Dict, List

from pydantic import BaseModel

from .model import Model
from .destination import Destination


class Flow(BaseModel):
    model: Model
    name: str = ""
    version: str = ""
    destinations: List[Destination] = []
    triggers: List[Dict[str, str]] = []
    priority: int = 0
    return_to_sender: bool = False
