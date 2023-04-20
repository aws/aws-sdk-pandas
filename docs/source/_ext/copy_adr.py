import shutil
from pathlib import Path


def setup(app):
    file_dir = Path(__file__).parent

    source_dir = file_dir.joinpath("../../../adr").resolve()
    destination_dir = file_dir.joinpath("../adr/").resolve()

    for file in source_dir.glob("*.md"):
        shutil.copy(file, destination_dir)
