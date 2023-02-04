from prefect import flow, task
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer

@flow
def test_flow():
    github_block = GitHub.load("handykapp-etl")
    docker_container_block = DockerContainer.load("handykapp-etl")

    print("THIS IS SERIOUSLY WORKING!")

if __name__ == "__main__":
    test_flow()
