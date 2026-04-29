.PHONY: pylint pytest

pylint:
	find . -iname "*.py" | xargs pylint --errors-only --disable=C0114,C0115,C0116 --extension-pkg-whitelist='pydantic'

pytest:
	pytest tests/ -v

all: pylint pytest
default: all
