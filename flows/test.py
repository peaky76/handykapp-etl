from prefect import flow, task

@flow
def test_flow():
    print("THIS IS SERIOUSLY WORKING!")

if __name__ == "__main__":
    test_flow()
