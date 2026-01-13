#!/bin/bash
# Setup script for lefthook git hooks

set -e

echo "🪝 Setting up lefthook for sbm-ingester..."

# Check if lefthook is installed
if ! command -v lefthook &> /dev/null; then
    echo "❌ lefthook not found. Installing..."

    # Detect OS and install
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            echo "📦 Installing via Homebrew..."
            brew install lefthook
        else
            echo "⬇️  Downloading binary..."
            curl -fsSL https://github.com/evilmartians/lefthook/releases/latest/download/lefthook_Darwin_$(uname -m) -o /usr/local/bin/lefthook
            chmod +x /usr/local/bin/lefthook
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        echo "⬇️  Downloading binary..."
        curl -fsSL https://github.com/evilmartians/lefthook/releases/latest/download/lefthook_Linux_x86_64 -o /usr/local/bin/lefthook
        chmod +x /usr/local/bin/lefthook
    else
        echo "❌ Unsupported OS. Please install lefthook manually:"
        echo "   https://github.com/evilmartians/lefthook#installation"
        exit 1
    fi
fi

# Verify installation
echo "✅ lefthook version: $(lefthook version)"

# Install git hooks
echo "🔧 Installing git hooks..."
lefthook install

echo ""
echo "✨ Lefthook setup complete!"
echo ""
echo "📝 Configured hooks:"
echo "   • pre-commit: ruff check, ruff format, trailing whitespace, YAML validation"
echo "   • pre-push: pytest, coverage check (≥90%)"
echo ""
echo "💡 Tips:"
echo "   • Skip hooks: LEFTHOOK=0 git commit"
echo "   • Skip tests: LEFTHOOK_EXCLUDE=pytest git push"
echo "   • Manual run: lefthook run pre-commit"
echo ""
echo "📖 See docs/LEFTHOOK.md for details"
