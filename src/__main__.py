import argparse
import json
import time
from joint_optimization import *
from typing import List, Dict

def copy_server_pool(server_pool: List[Server]) -> List[Server]:
    return [server.copy() for server in server_pool]

def evaluation(nslots: int, server_slots: List[int]):
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Input file for DAGs")
    args = parser.parse_args()

    print("Reproduction of 'Ditto'")

    with open(args.file, "r") as f:
        job_records = json.load(f)

    for job_record in job_records:
        job_name = job_record["name"]
        print(f"Processing job: {job_name}")

        stages: Dict[int, Stage] = {}
        stage_lookup: Dict[str, int] = {}
        edges: Dict[Tuple[int, int], float] = {}
        
        for stage in job_record["stages"]:
            stage_name = stage["name"]
            if stage_name in stage_lookup:
                raise Exception(f"Duplicate stage name: {stage_name}")
            stage_lookup[stage_name] = len(stages)
            stages[len(stages)] = Stage(stage["alpha"], stage["beta"])

        for stage in job_record["stages"]:
            stage_name = stage["name"]
            for child in stage["children"]:
                child_name = child["name"]
                if child_name not in stage_lookup:
                    raise Exception(f"Stage {child_name} not found")
                edges[(stage_lookup[stage_name], stage_lookup[child_name])] = child["weight"]

        # print("Stages: {}".format(stages))
        # print("Edges: {}".format(edges))

        job = Job(stages, edges, nslots)

        # test legality: sum(server_pool) >= nslots
        total_slots = sum(n for n in server_slots)
        assert total_slots >= nslots
        server_pool = [Server(n) for n in server_slots]

        start_time_ditto = time.time()
        ditto_time = joint_optimization(job.copy(), copy_server_pool(server_pool), Strategy.DITTO)
        end_time_ditto = time.time()

        start_time_ratio = time.time()
        ratio_time = joint_optimization(job.copy(), copy_server_pool(server_pool), Strategy.RATIO)
        end_time_ratio = time.time()

        start_time_average = time.time()
        average_time = joint_optimization(job.copy(), copy_server_pool(server_pool), Strategy.AVERAGE)
        end_time_average = time.time()

        print(f"Ditto: {ditto_time}, execution time: {end_time_ditto - start_time_ditto}")
        print(f"Ratio: {ratio_time}, execution time: {end_time_ratio - start_time_ratio}")
        print(f"Average: {average_time}, execution time: {end_time_average - start_time_average}")
        print()

def main():
    nslots = 120
    server_slots = [
        16,
        29,
        25,
        13,
        31,
        47,
        33,
        24,
        4,
        26,
        16,
        1,
        33,
        20,
    ]
    # print(len(server_slots))
    evaluation(nslots, server_slots)

    print("===========================")

    server_slots = [
        12,
        11,
        15,
        13,
        18,
        17,
        19,
        14,
        24,
        16,
        16,
        11,
        13,
        10,
    ]
    evaluation(nslots, server_slots)

if __name__ == "__main__":
    main()
