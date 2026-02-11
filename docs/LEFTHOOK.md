# Lefthook Setup Guide

## Installation

Lefthook is a Go binary, not a Python package. Choose one installation method:

### Option 1: Homebrew (macOS/Linux)
```bash
brew install lefthook
```

### Option 2: npm (if Node.js available)
```bash
npm install -g @evilmartians/lefthook
```

### Option 3: Direct Download
```bash
# Download binary for your platform
curl -fsSL https://github.com/evilmartians/lefthook/releases/latest/download/lefthook_$(uname -s)_$(uname -m) -o /usr/local/bin/lefthook
chmod +x /usr/local/bin/lefthook
```

### Option 4: Python Wrapper
```bash
uv add --optional dev lefthook
```

## Setup

After installing lefthook, initialize it in the repository:

```bash
# Install git hooks
lefthook install

# Verify installation
lefthook version
```

## Usage

### Manual Execution

```bash
# Run all pre-commit hooks manually
lefthook run pre-commit

# Run specific hook
lefthook run pre-commit ruff-check

# Run pre-push hooks
lefthook run pre-push
```

### Skip Hooks

```bash
# Skip all hooks for one commit
LEFTHOOK=0 git commit -m "skip hooks"

# Skip specific hooks
LEFTHOOK_EXCLUDE=pytest git push

# Skip tests on pre-push
LEFTHOOK_EXCLUDE=pytest,coverage git push
```

## Configured Hooks

### Pre-commit (runs on `git commit`)
- ✅ **ruff-check** - Lint Python files with auto-fix
- ✅ **ruff-format** - Format Python files
- ✅ **trailing-whitespace** - Check for trailing spaces
- ✅ **yaml-check** - Validate YAML syntax

All pre-commit hooks run in **parallel** for speed.

### Pre-push (runs on `git push`)
- 🧪 **pytest** - Run all tests
- 📊 **coverage** - Check test coverage (must be ≥90%)

## Troubleshooting

### Hook fails but git command succeeds
```bash
# Check hook output
lefthook run pre-commit --verbose

# Bypass hooks (not recommended)
git commit --no-verify -m "emergency fix"
```

### Hooks are slow
```bash
# Run only ruff checks, skip others
lefthook run pre-commit ruff-check ruff-format
```

### Update hooks after config change
```bash
lefthook install --force
```

## CI Integration

Lefthook can also run in CI:

```yaml
# .github/workflows/main.yml
- name: Install lefthook
  run: |
    curl -fsSL https://github.com/evilmartians/lefthook/releases/latest/download/lefthook_Linux_x86_64 -o /usr/local/bin/lefthook
    chmod +x /usr/local/bin/lefthook

- name: Run pre-commit checks
  run: lefthook run pre-commit

- name: Run pre-push checks
  run: lefthook run pre-push
```

## Configuration

Edit `lefthook.yml` to customize hooks. See [documentation](https://github.com/evilmartians/lefthook/blob/master/docs/configuration.md).
