from typing import List, Dict, Tuple
from job import Stage

class Server:
    def __init__(self, total_slots: int) -> None:
        self.total_slots = total_slots
        self.available_slots = total_slots
        self.placed_stages: Dict[int, Stage] = {}
        # I suppose the resourse contraints is available_slots 
    
    def can_place(self, stage: Tuple[int, Stage]) -> bool:
        return self.available_slots >= stage[1].nslot
    
    def place(self, stage: Tuple[int, Stage]) -> None:
        assert self.can_place(stage)
        self.available_slots -= stage[1].nslot
        self.placed_stages[stage[0]] = stage[1]
    
    def copy(self):
        return Server(self.total_slots)
