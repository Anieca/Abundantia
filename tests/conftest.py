import pytest
from pytest import Config, Item, Parser


def pytest_addoption(parser: Parser) -> None:
    parser.addoption("--integration", action="store_true", default=False)


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "integration: mark integration test")


def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    if config.getoption("--integration"):
        return
    skip = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
