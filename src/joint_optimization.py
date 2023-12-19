from job import *
from bottom_up_dop import bottom_up_dop
from server import Server
from typing import List
from enum import Enum

class Strategy(Enum):
    DITTO = 0,
    AVERAGE = 1,
    RATIO = 2

def joint_optimization(job: Job, servers: List[Server], strategy: Strategy) -> float:
    '''
    job: Job is the job to be scheduled
    return: float is the total execution time of the job
    '''
    # TODO: Implement this function
    pass
