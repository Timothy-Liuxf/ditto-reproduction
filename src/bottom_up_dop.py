from job import *
from math import pow

def merge_stage(alpha_i:float, alpha_j:float, has_edge:bool):
    if has_edge:
        rate = pow(alpha_i/alpha_j,0.5)
        alpha_s = pow(pow(alpha_i,0.5)+pow(alpha_j,0.5),2)
        return alpha_s, rate    
    
    rate = alpha_i/alpha_j
    alpha_s = alpha_i+alpha_j
    return alpha_s,rate


def get_depth(stages: Dict[int,Stage], edges: Dict[Tuple[int, int], float])->Dict[int,int]:
    depth_dict = {}
    in_dict = {}
    out_dict = {}
    v_list = []
    for v in stages.keys():
        depth_dict[v] = -1
        v_list.append(v)
        in_dict[v] = 0
        out_dict[v] = []
    for i,j in edges.keys():
        in_dict[j] += 1
        out_dict[i].append(j)
    
    depth = 0
    while len(v_list):
        zero_list = []
        for v in v_list:
            if in_dict[v]==0:
                zero_list.append(v)
        for v in zero_list:
            v_list.remove(v)
            depth_dict[v]=depth
            for child in out_dict[v]:
                in_dict[child] -= 1
        depth += 1
    return depth_dict
    

def alloc_slot(nslot: int, rate: float, min_a: int, min_b: int):
    if min_a + min_b > nslot:
        print("Slot is not enough!")
        return 0,0
    aslot = int(nslot*rate/(rate+1))
    aslot = max(min_a,aslot)
    bslot = nslot - aslot
    bslot = max(min_b,bslot)
    aslot = nslot - bslot
    return aslot,bslot


def bottom_up_dop(job: Job):
    stages = job.stages
    edges = job.edges
    depth_dict = get_depth(stages,edges)
    layer_dict = {}
    rate_cross_layer_list = [] # cross layer rate, r[i]=l[i]/l[i+1]
    rate_inner_layer_list = [] # inner layer rate, r[i]=l[i]/l[i+1] (reverse)
    alpha_list = []

    max_depth = 0
    for v, depth in depth_dict.items():
        if depth not in layer_dict.keys():
            layer_dict[depth] = []
            
        layer_dict[depth].append(v)
        max_depth = max(max_depth,depth)
    
    # merge
    for i in range(0,max_depth+1):
        v_list = layer_dict[i]
        alpha_list.append(stages[v_list[0]].alpha)
        rate_inner_layer_list.append([])
        for j in range(1,len(v_list)):
            sj = v_list[j]
            alpha_list[i],rate = merge_stage(alpha_list[i],stages[sj].alpha,False)
            rate_inner_layer_list[i].append(rate)
    
    for i in range(0,max_depth):
        rate_cross_layer_list.append(0)
    for i in range(max_depth,0, -1):
        alpha_list[i-1],rate = merge_stage(alpha_list[i-1],alpha_list[i],True)
        rate_cross_layer_list[i-1] = rate

    #print(rate_cross_layer_list)
    #print(rate_inner_layer_list)
    #print(alpha_list[0])
        
    # split
    nslot = job.nslot
    v_num = len(stages)
    for i in range(0,max_depth):
        v_list = layer_dict[i]
        v_num -= len(v_list)
        aslot, bslot = alloc_slot(nslot,rate_cross_layer_list[i],len(v_list),v_num)
        left_num = len(v_list)
        for j in range(len(v_list)-2,-1,-1):
            left_num -= 1
            islot, jslot = alloc_slot(aslot,rate_inner_layer_list[i][j],left_num,1)
            stages[v_list[j+1]].nslot = jslot
            aslot = islot
        stages[v_list[0]].nslot = aslot
        nslot = bslot
    v_list = layer_dict[max_depth]
    left_num = len(v_list)
    for j in range(len(v_list)-2,-1,-1):
        left_num -= 1
        islot, jslot = alloc_slot(aslot,rate_inner_layer_list[max_depth][j],left_num,1)
        stages[v_list[j+1]].nslot = jslot
        nslot = islot
    stages[v_list[0]].nslot = aslot


if __name__ == "__main__":
    stages = {}
    edges = {}
    for i in range(0,9):
        stages[i]=Stage(1,0)
    edge_list = [(0,1),(0,5),(1,2),(1,3),(2,4),(5,4),(5,6),(4,8),(6,7)]
    for edge in edge_list:
        edges[edge] = 1
    depth_dict = get_depth(stages,edges)
    print(depth_dict)
    job = Job(stages,edges,100)
    bottom_up_dop(job)
    for key,s in job.stages.items():
        print(key,s.nslot)