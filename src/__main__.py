import argparse
import json
from joint_optimization import *
from typing import Dict

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Input file for DAGs")
    args = parser.parse_args()

    print("Reproduction of 'Ditto'")

    with open(args.file, "r") as f:
        job_records = json.load(f)

    for job_record in job_records:
        try:
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
                for child in stage["children"]:
                    if child not in stage_lookup:
                        raise Exception(f"Stage {child} not found")
                    edges[(stage_lookup[stage_name], stage_lookup[child["name"]])] = child["weight"]

            job = Job(stages, edges, 100) # TODO: Change nslot
            print("Ditto: {}".format(joint_optimization(job.copy(), [], Strategy.DITTO)))
            print("Average: {}".format(joint_optimization(job.copy(), [], Strategy.AVERAGE)))
            print("Ratio: {}".format(joint_optimization(job.copy(), [], Strategy.RATIO)))
            print()
        except Exception as e:
            print(e)
            print()

    f0 = joint_optimization(Job([], [], 0), [], Strategy.DITTO)
    f1 = joint_optimization(Job([], [], 0), [], Strategy.AVERAGE)
    f2 = joint_optimization(Job([], [], 0), [], Strategy.RATIO)
    print("Total execution time: {}".format(f0, f1, f2))

if __name__ == "__main__":
    main()
