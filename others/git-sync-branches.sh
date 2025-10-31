#!/bin/bash

# Exit on error
set -e

# The last argument is the target branch, all others are branches to sync
ALL_BRANCHES=("$@")
if [ ${#ALL_BRANCHES[@]} -lt 2 ]; then
    echo "Usage: $0 branch1 [branch2 ...] target-branch"
    echo "Example: $0 obsidian zettel main"
    exit 1
fi

# Pop the last element from the array
TARGET_BRANCH="${ALL_BRANCHES[@]:(-1)}"
BRANCHES=("${ALL_BRANCHES[@]:0:${#ALL_BRANCHES[@]}-1}")

# Store current branch
current_branch=$(git rev-parse --abbrev-ref HEAD)

echo "Syncing branches ${BRANCHES[@]} with $TARGET_BRANCH"

# Update all branches from remote
echo "Updating all branches from remote..."
for branch in "${ALL_BRANCHES[@]}"; do
    echo "Updating $branch..."
    git fetch origin "$branch:$branch" --update-head-ok
done

# Then merge all branches into the target branch
git checkout "$TARGET_BRANCH"
for branch in "${BRANCHES[@]}"; do
    echo "Merging $branch into $TARGET_BRANCH..."
    git merge "$branch" --no-edit
done

# Finally merge te target branch back into all branches
for branch in "${BRANCHES[@]}"; do
    echo "Merging $TARGET_BRANCH back into $branch..."
    git fetch . "$TARGET_BRANCH:$branch"
done

# Push all branches
for branch in "${ALL_BRANCHES[@]}"; do
    echo "Pushing $branch..."
    git push origin "$branch"
done

# Return to original branch
git checkout "$current_branch"

echo "All branches synced successfully!"
