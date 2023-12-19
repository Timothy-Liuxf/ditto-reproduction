import argparse
from joint_optimization import *

def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    print("Reproduction of 'Ditto'")
    f0 = joint_optimization(Job([], [], 0), [], Strategy.DITTO)
    f1 = joint_optimization(Job([], [], 0), [], Strategy.AVERAGE)
    f2 = joint_optimization(Job([], [], 0), [], Strategy.RATIO)
    print("Total execution time: {}".format(f0, f1, f2))

if __name__ == "__main__":
    main()
