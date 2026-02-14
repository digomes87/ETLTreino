import os


def test_smoke_environment():
    assert True


def test_repo_layout_exists():
    assert os.path.isdir("tests")
