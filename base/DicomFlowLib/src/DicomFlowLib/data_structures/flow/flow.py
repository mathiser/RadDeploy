from typing import Dict, List

from pydantic import BaseModel

from .model import Model
from .destination import Destination


class Flow(BaseModel):
    name: str = ""
    version: str = ""
    models: List[Model] = []
    destinations: List[Destination] = []
    triggers: List[Dict[str, str]] = []
    priority: int = 0
    return_to_sender_on_ports: List[int] = []
    optional_kwargs: Dict = {}
