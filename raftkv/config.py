import yaml, os


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    os.makedirs(cfg["data_dir"], exist_ok=True)
    return cfg
