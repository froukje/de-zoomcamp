from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image="froukje/prefect:zoom",
    image_pull_policy="ALWAYS",
    autoremove=True
    )

docker_block.save("zoom", overwrite=True")
