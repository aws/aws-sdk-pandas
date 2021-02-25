import json
from pathlib import Path


def setup(app):
    for f in Path("../tutorials").glob("*.ipynb"):
        with open(f"source/tutorials/{f.stem}.nblink", "w") as output_file:
            nb_link = {"path": f"../../../tutorials/{f.name}", "extra-media": ["../../../tutorials/_static"]}
            json.dump(nb_link, output_file)
