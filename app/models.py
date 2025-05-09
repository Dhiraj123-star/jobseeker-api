from pydantic import BaseModel
from typing import List, Optional

class Job(BaseModel):
    title: str
    description: str
    tags: List[str]
    location: Optional[str] = None
    company: Optional[str] = None