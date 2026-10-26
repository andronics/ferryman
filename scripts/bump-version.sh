#!/bin/bash
#
# Version Bumping Script for ferryman
#
# Usage: ./scripts/bump-version.sh [major|minor|patch]
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get current directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Get current version from pyproject.toml (the package reads its version
# from its own metadata, so no Python file holds a copy)
PYPROJECT_FILE="$ROOT_DIR/pyproject.toml"
LOCK_FILE="$ROOT_DIR/uv.lock"

if [ ! -f "$PYPROJECT_FILE" ]; then
    echo -e "${RED}Error: Cannot find $PYPROJECT_FILE${NC}"
    exit 1
fi

CURRENT_VERSION=$(grep -oP '^version = "\K[^"]+' "$PYPROJECT_FILE")

echo -e "${BLUE}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         ferryman Version Bumper             ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════╝${NC}"
echo
echo -e "${YELLOW}Current version: ${GREEN}v${CURRENT_VERSION}${NC}"
echo

# Parse current version
IFS='.' read -r MAJOR MINOR PATCH <<< "${CURRENT_VERSION%-*}"

# Determine bump type
BUMP_TYPE=$1

if [ -z "$BUMP_TYPE" ]; then
    echo "Select version bump type:"
    echo -e "  ${GREEN}1${NC}) patch   (${CURRENT_VERSION} → ${MAJOR}.${MINOR}.$((PATCH + 1)))"
    echo -e "  ${GREEN}2${NC}) minor   (${CURRENT_VERSION} → ${MAJOR}.$((MINOR + 1)).0)"
    echo -e "  ${GREEN}3${NC}) major   (${CURRENT_VERSION} → $((MAJOR + 1)).0.0)"
    echo -e "  ${RED}4${NC}) cancel"
    echo
    read -r -p "Enter choice [1-4]: " choice

    case $choice in
        1) BUMP_TYPE="patch" ;;
        2) BUMP_TYPE="minor" ;;
        3) BUMP_TYPE="major" ;;
        4|*) echo -e "${RED}Cancelled${NC}"; exit 0 ;;
    esac
fi

# Calculate new version
case $BUMP_TYPE in
    major)
        NEW_VERSION="$((MAJOR + 1)).0.0"
        ;;
    minor)
        NEW_VERSION="${MAJOR}.$((MINOR + 1)).0"
        ;;
    patch)
        NEW_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))"
        ;;
    *)
        echo -e "${RED}Error: Invalid bump type. Use 'major', 'minor', or 'patch'${NC}"
        exit 1
        ;;
esac

NEW_TAG="v${NEW_VERSION}"

echo
echo -e "${YELLOW}Bump type: ${GREEN}${BUMP_TYPE}${NC}"
echo -e "${YELLOW}New version: ${GREEN}${NEW_TAG}${NC}"
echo

# Confirm
read -r -p "$(echo -e "${YELLOW}Create release ${GREEN}${NEW_TAG}${YELLOW}? [y/N]: ${NC}")" confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo -e "${RED}Cancelled${NC}"
    exit 0
fi

# Check git status
if [ -n "$(git status --porcelain)" ]; then
    echo -e "${RED}Error: Working directory has uncommitted changes${NC}"
    echo "Please commit or stash your changes first."
    exit 1
fi

# Check we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo -e "${YELLOW}Warning: Not on 'main' branch (current: ${CURRENT_BRANCH})${NC}"
    read -r -p "Continue anyway? [y/N]: " continue_anyway
    if [[ ! "$continue_anyway" =~ ^[Yy]$ ]]; then
        echo -e "${RED}Cancelled${NC}"
        exit 0
    fi
fi

# Update version in pyproject.toml, and uv.lock to match
echo -e "${BLUE}Updating version files...${NC}"
sed -i "s/^version = .*/version = \"${NEW_VERSION}\"/" "$PYPROJECT_FILE"
(cd "$ROOT_DIR" && uv lock --quiet)

# Show diff
echo -e "${YELLOW}Changes:${NC}"
git diff "$PYPROJECT_FILE" "$LOCK_FILE"

# Commit version change
echo -e "${BLUE}Committing version change...${NC}"
git add "$PYPROJECT_FILE" "$LOCK_FILE"
git commit -m "chore: Bump version to ${NEW_VERSION}"

# Create and push tag
echo -e "${BLUE}Creating tag ${NEW_TAG}...${NC}"
git tag -a "$NEW_TAG" -m "Release ${NEW_TAG}"

echo
echo -e "${GREEN}✓ Version bumped to ${NEW_TAG}${NC}"
echo
echo "Next steps:"
echo -e "  ${YELLOW}1.${NC} Review the commit and tag:"
echo -e "     git log -1"
echo -e "     git tag -n ${NEW_TAG}"
echo
echo -e "  ${YELLOW}2.${NC} Push to trigger release:"
echo -e "     ${GREEN}git push && git push --tags${NC}"
echo
echo -e "  ${YELLOW}3.${NC} Monitor GitHub Actions for release build:"
echo -e "     https://github.com/andronics/ferryman/actions"
echo
