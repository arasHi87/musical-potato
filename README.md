# Simple storage interface template

This is a simple storage interface template for students in NCKU's system administration course.

## Getting started

TA has already completed the section regarding the web app, so your focus should be on implementing the remaining part of `api/storage.py`.

### Prerequisites

- python = '^3.8'
- poetry = '^1.4.0'
- make = '^1.4.0'

### Installation

1. The initialization environment will assist you in setting up Poetry, installing dependencies, and copying a new .env file.

```
make init
```

2. Optionally, you can install a pre-commit hook that ensures code quality each time you commit.

```
poetry run pre-commit install
```

3. Enter the environment and begin your development work.

```
poetry shell
```

4. Optionally, you can run the simple test case written by the TA to identify any potential issues in your code.

```
make test
```

5. Commence development of the API service, which will run at `http://localhost:8000`.

```
cd api && poetry run uvicorn app:APP --reload --host 0.0.0.0
```

### Formatting & Linting

Black, isort, flake8, and pylint are used for formatting and linting in this project. You can customize these settings in the `setup.cfg` file.

```
make lint format
```
