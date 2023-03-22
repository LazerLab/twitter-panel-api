
EXAMPLES = examples
SRC = panel_api
TEST = test
ALL = $(SRC) $(TEST) $(EXAMPLES)

deps: requirements.txt
	pip install -r requirements.txt

# Formatting

format-black:
	@black $(ALL)

format-isort:
	@isort $(ALL)

format: format-black format-isort

# Linting

lint-black:
	@black $(ALL) --check

lint-isort:
	@isort $(ALL) --check

lint-flake8:
	@flake8 $(ALL)

lint-doc:
	@pylint $(SRC)

lint-mypy:
	@mypy $(ALL)

lint: lint-black lint-isort lint-flake8 lint-doc lint-mypy

# Testing

test-unit:
	@pytest $(TEST)

test: test-unit