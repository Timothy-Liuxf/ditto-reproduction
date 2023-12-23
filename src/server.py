from typing import List
from job import Stage

class Server:
    def __init__(self, total_slots: int) -> None:
        self.total_slots = total_slots
        self.available_slots = total_slots
        self.placed_stages: List[Stage] = []
        # I suppose the resourse contraints is available_slots 
    
    def can_place(self, stage: Stage) -> bool:
        return self.available_slots >= stage.nslot
    
    def place(self, stage: Stage) -> None:
        assert self.can_place(stage)
        self.available_slots -= stage.nslot
        self.placed_stages.append(stage)
