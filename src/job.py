from typing import List, Tuple, Dict

class Stage:
    def __init__(self, alpha: float, beta: float, nslot: int = 0) -> None:
        self.alpha = alpha
        self.beta = beta
        self.nslot = nslot

class VirtualStage(Stage):
    def __init__(self, alpha: float, beta: float) -> None:
        super().__init__(alpha, beta)
        # TODO: Add more fields

class Job:
    def __init__(self, stages: Dict[int, Stage], edges: Dict[Tuple[int, int], float], nslot: int) -> None:
        '''
        stages: List[Stage] is a list of stages in the job
        edges: Dict[Tuple[int, int], float] is a dictionary of edges between stages
            edges[(i, j)] < 0 iff there is NOT an edge from stage i to stage j
            otherwise edges[(i, j)] is the weight of the edge from stage i to stage j
        '''
        self.stages = stages
        self.edges = edges
        self.nslot = nslot

    def copy(self):
        return Job(self.stages.copy(), self.edges.copy(), self.nslot)
