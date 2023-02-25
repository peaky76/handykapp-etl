# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))


# @flow
def test():
    print("Hello, world!")


if __name__ == "__main__":
    test()
