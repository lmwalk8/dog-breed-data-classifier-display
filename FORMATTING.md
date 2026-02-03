# Code Formatting and Linting Guide

This project uses automated code formatting and linting tools to maintain consistent code style.

## Tools Used

- **Black**: Automatic code formatter (line length: 88 characters)
- **isort**: Import statement organizer (compatible with Black)
- **flake8**: Style guide enforcement (PEP 8)
- **pre-commit**: Git hooks for automatic formatting before commits

## Setup

### 1. Install Development Dependencies

```
pip install -r requirements-dev.txt
```

### 2. Install Pre-commit Hooks (Recommended)

```
pre-commit install
```

This will automatically format and lint your code before each commit.

## Usage

### Format Code with Black

Format all Python files:
```
black .
```

Format a specific file:
```
black scripts/transform.py
```

Check what would be changed (without modifying files):
```
black --check .
```

### Sort Imports with isort

Sort imports in all files:
```
isort .
```

Sort imports in a specific file:
```
isort scripts/transform.py
```

Check what would be changed:
```
isort --check-only .
```

### Lint with flake8

Run flake8 on all files:
```
flake8 .
```

Run flake8 on a specific directory:
```
flake8 scripts/
```

### Run All Formatting Tools

```
# Format code
black .
isort .

# Check for issues
flake8 .
```

## Pre-commit Hooks

If you've installed pre-commit hooks, they will automatically run when you commit:

- Remove trailing whitespace
- Ensure files end with newline
- Format code with Black
- Sort imports with isort
- Run flake8 checks

To run hooks manually on all files:
```
pre-commit run --all-files
```

### Uninstalling Pre-commit Hooks

To remove pre-commit hooks (they will no longer run automatically):

```
pre-commit uninstall
```

This removes the git hooks but keeps the `.pre-commit-config.yaml` file, so you can reinstall later with `pre-commit install`.

### Bypassing Pre-commit Hooks

If you need to commit without running the hooks (not recommended, but sometimes necessary):

```
git commit --no-verify
```

Or use the short form:
```
git commit -n
```

**Note**: Bypassing hooks should be used sparingly, as it may result in unformatted code being committed.

## Configuration

- **Black**: Configured in `pyproject.toml` (line length: 88)
- **isort**: Configured in `pyproject.toml` (compatible with Black)
- **flake8**: Configured in `.flake8` (line length: 88)
