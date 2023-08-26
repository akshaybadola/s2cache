from pathlib import Path

from s2cache.config import default_config, load_config


def test_config_init():
    config = default_config()
    for k in config._keys:
        assert k in config
    config.cache_dir = None
    config["cache_dir"] = "cache_data"


def test_config_load():
    config = default_config()
    load_config(config, Path(__file__).parent.joinpath("config.yaml"))
    assert config.cache_dir == "cache_data"


    config = default_config()
    load_config(config, Path(__file__).parent.joinpath("diff_config.yaml"))
    assert config.citations.limit == 55
