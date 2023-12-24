
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
        Eg = None
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
                Eg = edge
                # si = edge[0]
                # sj = edge[1]

                # Update Dop ???
                # new_grouped_stages = grouped_stages.copy()
                # for grouped_stage in new_grouped_stages:
                # pass

                # place check if current grouped_stages can be placed into the server list
                # can put them into the server if possible
                if place(servers, job, Eg):
                    Eu.remove(edge)
                    break
                else:
                    # Undo grouping 𝑠𝑖 and 𝑠𝑗, and restore 𝐷𝑜𝑃
                    # Undo line 11 and 12
                    job.edges[edge] = tmp
                    Eg = None

            # if No edge in E𝑢 is grouped in the above loop then break
            if len(Eu) == Eu_len:
                break

    elif strategy == Strategy.AVERAGE:

        # All stages have the same Dop
        for id, stage in job.stages.items():
            stage.nslot = round(job.nslot / len(list(job.stages.keys())))

            # Randomly place each stage into available server
            for server in servers:
                if server.can_place((id, stage)):
                    server.place((id, stage))

    elif strategy == Strategy.RATIO:

        # Compute k, which is the propotion
        total_alpha = 0
        for id, stage in job.stages.items():
            total_alpha += stage.alpha

        # Stage Dop is propotional to the stage alpha value
        for id, stage in job.stages.items():
            try:
                stage.nslot = round(job.nslot * stage.alpha / total_alpha)
            except ZeroDivisionError:
                stage.nslot = 1

            # Randomly place each stage into available server
            for server in servers:
                if server.can_place((id, stage)):
                    server.place((id, stage))

    '''
    For JCT optimization, the weight of node 𝑠𝑖 is 𝐶(𝑠𝑖), and the weight of (𝑠𝑖, 𝑠𝑗) is 𝑊 (𝑠𝑖) + 𝑅(𝑠𝑗).
    For cost optimization, the node weight is 𝑀(𝑠𝑖)𝐶(𝑠𝑖), and the edge weight is 𝑀(𝑠𝑖)𝑊 (𝑠𝑖) + 𝑀(𝑠𝑗)𝑅(𝑠𝑗). 
    '''
    # Compute total time
    total_execution_time = 0

    # Find current graph critical path to compute total time
    critical_path_edge_attributes = longest_path_dag_with_weights_and_path(job.stages, job.edges)

    for i in range(len(critical_path_edge_attributes)):
        start_stage = job.stages[critical_path_edge_attributes[i][1]]
        end_stage = job.stages[critical_path_edge_attributes[i][1]]
        weight = critical_path_edge_attributes[i][2]

        # Start node
        if i == 0:
            total_execution_time += start_stage.alpha / start_stage.nslot + start_stage.beta

        total_execution_time += weight
        total_execution_time += end_stage.alpha / end_stage.nslot + end_stage.beta

    return total_execution_time


def build_group_stages(job : Job, Eg : List[Tuple[int, int]]) -> Set[Stage]:
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
'''
def place(servers : List[Server], job : Job, Eg : Tuple[int, int]) -> bool:

    # sort server based on the curret available slots:
    sorted_servers = sorted(servers, key=lambda server: server.available_slots, reverse=True)

    start_placed_server = -1
    end_placed_server = -1
    start_stage = job.stages[Eg[0]]
    end_stage = job.stages[Eg[1]]

    # Check if stage is placed somewhere in the server list
    for i, server in enumerate(servers):
        if Eg[0] in server.placed_stages:
            start_placed_server = i
        if Eg[1] in server.placed_stages:
            end_placed_server = i

    chosen_server = None
    if start_placed_server >= 0 and end_placed_server >= 0:
        return True

    elif start_placed_server < 0 and end_placed_server < 0:
        total_slots_need = start_stage.nslot + end_stage.nslot

        # Place the group into the server with the nearest function slot number
        for server in sorted_servers:
            if server.available_slots >= total_slots_need:
                chosen_server = server
            else:
                if chosen_server:
                    chosen_server.available_slots -= total_slots_need
                    chosen_server.placed_stages[Eg[0]] = start_stage
                    chosen_server.placed_stages[Eg[1]] = end_stage

    elif start_placed_server >= 0 and end_placed_server < 0:
        total_slots_need = end_stage.nslot

        if servers[start_placed_server].available_slots >= total_slots_need:
            chosen_server = servers[start_placed_server]
            chosen_server.available_slots -= total_slots_need
            chosen_server.placed_stages[Eg[1]] = end_stage

    elif start_placed_server < 0 and end_placed_server >= 0:
        total_slots_need = start_stage.nslot

        if servers[end_placed_server].available_slots >= total_slots_need:
            chosen_server = servers[end_placed_server]
            chosen_server.available_slots -= total_slots_need
            chosen_server.placed_stages[Eg[0]] = start_stage

    if not chosen_server: return False
    return True


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
        critical_path_edge_attributes = longest_path_dag_with_weights_and_path(job.stages, job.edges)

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
    path length includes both node cost and weight cost
    return : list[(si, sj, w)]
'''
def longest_path_dag_with_weights_and_path(nodes : Dict[int, Stage], edges : Dict[Tuple[int, int], float]) -> List[Tuple[int, int, float]]:

    # find path starting points
    graph = {node: [] for node in nodes.keys()}
    in_degree = {node: 0 for node in nodes.keys()}
    edge_costs = {node: [] for node in nodes.keys()}
    node_costs = {node: stage.alpha / stage.nslot + stage.beta for node, stage in nodes.items()}

    for edge, weight in edges.items():
        in_degree[edge[1]] += 1
        graph[edge[0]].append(edge[1])
        edge_costs[edge[0]].append(weight)

    path = {node: [] for node in nodes.keys()}
    distance_with_cost = {node: 0 for node in nodes.keys()}
    start_nodes = [node for node in nodes.keys() if in_degree[node] == 0]

    for root_node in start_nodes:
        path[root_node].append(root_node)
        distance_with_cost[root_node] = node_costs[root_node]
        dfs_with_weights_and_path(root_node, graph, edge_costs, node_costs, in_degree, path, distance_with_cost)

    max_node = max(distance_with_cost, key=distance_with_cost.get)
    critical_path = path[max_node]

    critical_path_edge_attributes = []
    for i in range(len(critical_path) - 1):
        critical_path_edge = (critical_path[i], critical_path[i+1])
        critical_path_edge_attributes.append((critical_path[i], critical_path[i+1], edges[critical_path_edge]))

    return critical_path_edge_attributes


def dfs_with_weights_and_path(root_node, graph, edge_costs, node_costs, in_degree, path, distance_with_cost):

    for i, neighbor in enumerate(graph[root_node]):

        # print(in_degree)
        # print(root_node)
        # print(neighbor, graph[root_node])
        # print(neighbor, in_degree[neighbor])
        in_degree[neighbor] -= 1

        new_distance = distance_with_cost[root_node] + edge_costs[root_node][i] + node_costs[neighbor]

        if new_distance > distance_with_cost[neighbor]:
            path[neighbor] = path[root_node] + [neighbor]
            distance_with_cost[neighbor] = new_distance

        if in_degree[neighbor] == 0:
            dfs_with_weights_and_path(neighbor, graph, edge_costs, node_costs, in_degree, path, distance_with_cost)


'''
    Issues:
        1. greedy grouping and placement without considering edge dependency relation
        2. Update A with bottom_up_dom in each iteration or not
'''
