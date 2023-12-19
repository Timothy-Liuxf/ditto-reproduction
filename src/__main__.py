import argparse
from joint_optimization import *

def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    print("Reproduction of 'Ditto'")
    f = joint_optimization(Job([], [], 0), [])
    print("Total execution time: {}".format(f))

if __name__ == "__main__":
    main()
