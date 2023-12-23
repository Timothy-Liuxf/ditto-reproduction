from typing import List, Tuple, Dict

class Stage:
    def __init__(self, alpha: float, beta: float, nslot: int = 0) -> None:
        self.alpha = alpha  # This implementation splits A[s] here
        self.beta = beta
        self.nslot = nslot  # This is Dop

class VirtualStage(Stage):
    def __init__(self, alpha: float, beta: float) -> None:
        super().__init__(alpha, beta)
        # TODO: Add more fields

class Job:
    def __init__(self, stages: Dict[int, Stage], edges: Dict[Tuple[int, int], float], nslot: int) -> None:
        '''
        stages: Dict[int, Stage] is a dict of stages in the job
            key of the dict: stage id
        edges: Dict[Tuple[int, int], float] is a dictionary of edges between stages
            Tuple[int, int] represents the id connection of stages
            edges[(i, j)] < 0 iff there is NOT an edge from stage i to stage j
            otherwise edges[(i, j)] is the weight of the edge from stage i to stage j
        '''
        self.stages = stages    # This is V
        self.edges = edges      # This is E
        self.nslot = nslot      # This is Dop

    def copy(self):
        return Job(self.stages.copy(), self.edges.copy(), self.nslot)
