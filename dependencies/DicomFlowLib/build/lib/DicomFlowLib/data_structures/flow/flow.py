
from typing import Dict, List

from pydantic import BaseModel

from .model import Model
from .destination import Destination


class Flow(BaseModel):
    model: Model
    destinations: List[Destination] = []
    triggers: List[Dict[str, str]] = []

