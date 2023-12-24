import argparse
import json
from joint_optimization import *
from typing import List, Dict

def copy_server_pool(server_pool: List[Server]) -> List[Server]:
    return [server.copy() for server in server_pool]

def main():
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

        nslots = 100
        job = Job(stages, edges, nslots)
        server_slots = [
            16,
            9,
            5,
            13,
            11,
            7,
            3,
            24,
            4,
            6,
            16,
            1,
            3,
            20,
        ]
        # test legality: sum(server_pool) >= nslots
        total_slots = sum(n for n in server_slots)
        assert total_slots >= nslots
        server_pool = [Server(n) for n in server_slots]

        print("Ditto: {}".format(joint_optimization(job.copy(), copy_server_pool(server_pool), Strategy.DITTO)))
        print("Average: {}".format(joint_optimization(job.copy(), copy_server_pool(server_pool), Strategy.AVERAGE)))
        print("Ratio: {}".format(joint_optimization(job.copy(), copy_server_pool(server_pool), Strategy.RATIO)))
        print()

if __name__ == "__main__":
    main()
