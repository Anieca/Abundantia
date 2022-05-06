import pytest
from pytest import Config, Item, Parser


def pytest_addoption(parser: Parser) -> None:
    parser.addoption("--integration", action="store_true", default=False)
    parser.addoption("--database", action="store_true", default=False)
    parser.addoption("--exchange", action="store_true", default=False)


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "integration: mark integration test")
    config.addinivalue_line("markers", "database: mark database test")
    config.addinivalue_line("markers", "exchange: mark exchange test")


def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    if not config.getoption("--integration"):
        skip = pytest.mark.skip(reason="need --integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)

    if not config.getoption("--database"):
        skip = pytest.mark.skip(reason="need --database option to run")
        for item in items:
            if "database" in item.keywords:
                item.add_marker(skip)

    if not config.getoption("--exchange"):
        skip = pytest.mark.skip(reason="need --exchange option to run")
        for item in items:
            if "exchange" in item.keywords:
                item.add_marker(skip)
