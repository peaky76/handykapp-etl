from prefect import flow

# To allow running as a script
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


@flow
def test():
    print("Hello, world!")


if __name__ == "__main__":
    test()
