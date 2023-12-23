
'''
Joint optimization. Algorithm 3 shows the pseudo-code of the
joint optimization for JCT. It co-optimizes the parallelism configuration and the stage 
grouping in an iterative manner. Initially,
each stage is regarded as a group. In each iteration, the algorithm
follows the greedy grouping order to traverse the edges in the DAG.
It attempts to group the two stages connected by an edge. Then, it
uses the DoP ratio computing algorithm, implemented by the function BOTTOM_UP_DOP as 
shown in Algorithm 1, to figure out the
optimal parallelism configuration based on the stage groups and
the parallelized time characteristics. The function CAN_PLACE
implements the best fit algorithm to check the placement feasibility
under the resource constraints R. After the stage grouping and
parallelism configuration are determined, the algorithm tries to
place the stage groups onto the physical servers. If the placement
check fails, the algorithm backtracks and breaks the group into the
original stages. Otherwise, the new group is retained. Subsequently,
it traverses other consecutive stages in next iterations and repeats
the above process until no stages can be grouped.
The joint optimization for cost is similar to that for JCT. The only
difference is that the optimal DoP ratios and the greedy grouping
order are computed based on the cost model, as described in § 4.2
and § 4.3, respectively.
'''

from job import *
from bottom_up_dop import bottom_up_dop
from server import Server
from typing import List, Set
from enum import Enum

class Strategy(Enum):
    DITTO = 0,
    AVERAGE = 1,
    RATIO = 2

'''
Let V be the set of all stages and E be the set of all data dependencies in the DAG.
The function CAN_PLACE implements the best fit algorithm to check the placement feasibility
under the resource constraints R.
Array 𝐴[𝑠 ] stores the parallelized time parameters (𝛼) of all stages

function JOINT_ITERATIVE_OPTIMIZATION(V, E, R)
2: // Initialize 𝐷𝑜𝑃 and update the parameters (𝐴, 𝜔) based on 𝐷𝑜𝑃
3: 𝐷𝑜𝑃 ← BOTTOM_UP_DOP(V, E, 𝐴)
4: UPDATE_PARAMS(𝐴, 𝜔, 𝐷𝑜𝑃)
5: // E𝑔 and E𝑢 store grouped and ungrouped edges, respectively
6: E𝑔 ← ∅, E𝑢 ← E
7: while E𝑢 ≠ ∅ do
8: Sort E𝑢 in greedy grouping order mentioned in § 4.3
9: for (𝑠𝑖, 𝑠𝑗 ) ∈ E𝑢 do
10: // Try grouping 𝑠𝑖 and 𝑠𝑗
11: 𝜔𝑖 𝑗 ← 0, E𝑔 ← E𝑔 ∪ { (𝑠𝑖, 𝑠𝑗 ) }
12: 𝐷𝑜𝑃 ← BOTTOM_UP_DOP(V, E, 𝐴)
13: if CAN_PLACE(𝐷𝑜𝑃, E𝑔, R) then
14: UPDATE_PARAMS(𝐴, 𝜔, 𝐷𝑜𝑃)
15: E𝑢 ← E𝑢 − { (𝑠𝑖, 𝑠𝑗 ) }
16: break
17: else
18: // Undo grouping 𝑠𝑖 and 𝑠𝑗, and restore 𝐷𝑜𝑃
19: Undo line 11 and 12
20: if No edge in E𝑢 is grouped in the above loop then
21: break
'''

def joint_optimization(job: Job, servers: List[Server], strategy: Strategy) -> float:
    '''
    job: Job is the job to be scheduled
    return: float is the total execution time of the job
    '''
    if strategy == Strategy.DITTO:

        # Initialize 𝐷𝑜𝑃 and update the parameters (𝐴, 𝜔) based on 𝐷𝑜𝑃
        bottom_up_dop(job)

        # E𝑔 and E𝑢 store grouped and ungrouped edges, respectively
        Eg = []
        Eu = list(job.edges.keys())
        # stage_ids = list(job.stages.keys())
        # grouped_stages = [[id] for id in stage_ids]

        while Eu:
            # Sort E𝑢 in greedy grouping order mentioned in § 4.3
            Eu = greedy_group(job.copy())
            Eu_len = len(Eu)

            for edge in Eu:
                # Try grouping 𝑠𝑖 and 𝑠𝑗
                tmp = job.edges[edge]
                if tmp == 0: break

                job.edges[edge] = 0
                Eg.append(edge)
                # si = edge[0]
                # sj = edge[1]

                # new_grouped_stages = grouped_stages.copy()
                # for grouped_stage in new_grouped_stages:
                # pass

                # can_place check if current grouped_stages can be placed into the server list if possible
                if can_place(servers, job, Eg):
                    Eu.remove(edge)
                    break
                else:
                    # Undo grouping 𝑠𝑖 and 𝑠𝑗, and restore 𝐷𝑜𝑃
                    # Undo line 11 and 12
                    job.edges[edge] = tmp
                    Eg.remove(edge)

            # if No edge in E𝑢 is grouped in the above loop then break
            if len(Eu) == Eu_len:
                break

            # place current grouped stages into the server
            place(servers, job, Eg)
            Eg = []

    elif strategy == Strategy.AVERAGE:

        # All stages have the same Dop
        for stage in job.stages.values():
            stage.nslot = 5

            # Randomly place each stage into available server
            for server in servers:
                if server.can_place(stage):
                    server.place(stage)

    elif strategy == Strategy.RATIO:

        # Stage Dop is propotional to the stage alpha value
        for stage in job.stages.values():
            stage.nslot = round(stage.alpha * 10)

            # Randomly place each stage into available server
            for server in servers:
                if server.can_place(stage):
                    server.place(stage)

    '''
    For JCT optimization, the weight of node 𝑠𝑖 is 𝐶(𝑠𝑖), and the weight of (𝑠𝑖, 𝑠𝑗) is 𝑊 (𝑠𝑖) + 𝑅(𝑠𝑗).
    For cost optimization, the node weight is 𝑀(𝑠𝑖)𝐶(𝑠𝑖), and the edge weight is 𝑀(𝑠𝑖)𝑊 (𝑠𝑖) + 𝑀(𝑠𝑗)𝑅(𝑠𝑗). 
    '''
    # Compute total time
    total_execution_time = 0

    for server in servers:
        for stage in server.placed_stages:
            execution_time = (stage.alpha / stage.nslot + stage.beta) * (server.total_slots - server.available_slots)
            if execution_time > total_execution_time:
                total_execution_time = execution_time

    return total_execution_time


def build_group_stages(job : Job, Eg : List[Tuple(int, int)]) -> Set[Stage]:
    group_stages = set()
    for edge in Eg:
        for s in [edge[0], edge[1]]:
            group_stages.add(job.stages[s])
    return group_stages


'''
    Check if the current group can be placed into a single server
    servers : Avaliable server with constraints
    job: current job
    Eg: current grouped stages represented with edges

    Note: grouped stages may not be fully connected
'''
def can_place(servers : List[Server], job : Job, Eg : List[Tuple(int, int)]) -> bool:

    group_stages = build_group_stages(job, Eg)
    for stage in group_stages.copy():
        # Check if s is placed somewhere in the server list
        for server in servers:
            if stage in server.placed_stages:
                group_stages.remove(stage)
                break

    total_slots_need = 0
    for stage in group_stages:
        total_slots_need += stage.nslot

    for server in servers:
        if server.available_slots >= total_slots_need:
            return True

    return False


def place(servers : List[Server], job : Job, Eg : List[Tuple(int, int)]) -> None:

    group_stages = build_group_stages(job, Eg)
    for stage in group_stages.copy():
        # Check if s is placed somewhere in the server list
        for server in servers:
            if stage in server.placed_stages:
                group_stages.remove(stage)
                break

    total_slots_need = 0
    for stage in group_stages:
        total_slots_need += stage.nslot

    # sort server based on the curret available slots:
    sorted_servers = sorted(servers, key=lambda server: server.available_slots, reverse=True)

    # Place the group into the server with the nearest function slot number
    chosen_server = None
    for server in sorted_servers:
        if server.available_slots >= total_slots_need:
            chosen_server = server
        else:
            chosen_server.available_slots -= total_slots_need
            chosen_server.placed_stages += group_stages


'''
function GREEDY_GROUP(V, E, R, 𝑜𝑏 𝑗 , 𝐷𝑜𝑃)
2: // 𝐷𝑜𝑃 stores the DoP of each stage
3: E𝑔 ← ∅
4: while E ≠ ∅ do
5: if 𝑜𝑏 𝑗 is JCT then
6: 𝐶𝑃 ←the critical path of the DAG (V, E)
7: (𝑠𝑖 , 𝑠𝑗 ) ←the edge with the largest weight in 𝐶𝑃
8: else
9: (𝑠𝑖 , 𝑠𝑗 ) ←the edge with the largest weight
10: // Try grouping 𝑠𝑖 and 𝑠𝑗 , and 𝜔𝑖 𝑗 is the weight of (𝑠𝑖 , 𝑠𝑗 )
11: 𝜔𝑖 𝑗 ← 0, E𝑔 ← E𝑔 ∪ { (𝑠𝑖 , 𝑠𝑗 ) }
12: if CAN_PLACE(𝐷𝑜𝑃, E𝑔, R) is false then
13: E𝑔 ← E𝑔 − { (𝑠𝑖 , 𝑠𝑗 ) }
14: E ← E − { (𝑠𝑖 , 𝑠𝑗 ) }
'''
def greedy_group(job : Job):

    Eg = []
    E = job.edges.copy()

    while E:
        # find the critical path of the DAG (V, E)
        critical_path_edge_attributes = longest_path_dag_with_weights_and_path(list(job.stages.keys()), job.edges)

        # find the edge with the largest weight in 𝐶𝑃
        max_weight = 0
        max_edge = (-1, -1)
        for edge in critical_path_edge_attributes:
            if edge[2] > max_weight:
                max_weight = edge[2]
                max_edge = (edge[0], edge[1])
        
        if max_weight == 0 : break

        # Try grouping 𝑠𝑖 and 𝑠𝑗 , and 𝜔𝑖 𝑗 is the weight of (𝑠𝑖 , 𝑠𝑗 )
        Eg.append(max_edge)
        job.edges[max_edge] = 0

        E.pop(max_edge)

    return Eg


'''
    Find critical path in current DAG
    return : list[(si, sj, w)]
'''
def longest_path_dag_with_weights_and_path(nodes : List[int], edges : Dict[Tuple[int, int], float]) -> List[Tuple(int, int, float)]:

    # find path starting points
    graph = {node: [] for node in nodes}
    in_degree = {node: 0 for node in nodes}
    edge_costs = {node: [] for node in nodes}

    for edge, weight in edges.items():
        in_degree[edge[1]] += 1
        graph[edge[0]].append(edge[1])
        edge_costs[edge[0]].append(weight)

    path = {node: [] for node in nodes}
    distance_with_cost = {node: 0 for node in nodes}
    start_nodes = [node for node in nodes if in_degree[node] == 0]

    for root_node in start_nodes:
        path[root_node].append(root_node)
        dfs_with_weights_and_path(root_node, graph, edge_costs, in_degree, path, distance_with_cost)

    max_node = max(distance_with_cost, key=distance_with_cost.get)
    critical_path = path[max_node]

    critical_path_edge_attributes = []
    for i in range(len(critical_path) - 1):
        critical_path_edge = (critical_path[i], critical_path[i+1])
        critical_path_edge_attributes.append((critical_path[i], critical_path[i+1], edges[critical_path_edge]))

    return critical_path_edge_attributes


def dfs_with_weights_and_path(root_node, graph, edge_costs, in_degree, path, distance_with_cost):

    for i, neighbor in enumerate(graph[root_node]):
        in_degree[neighbor] -= 1

        new_distance = distance_with_cost[root_node] + edge_costs[root_node][i]

        if new_distance > distance_with_cost[neighbor]:
            path[neighbor] = path[root_node] + [neighbor]
            distance_with_cost[neighbor] = new_distance

        if in_degree[neighbor] == 0:
            dfs_with_weights_and_path(neighbor, graph, in_degree, path, distance_with_cost, edge_costs)

