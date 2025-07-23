#!/bin/bash

cd /nobackup/h_cqs/shengq2/program/warp

# Ensure working in the correct repository directory
echo "Checking current directory..."
pwd

# Add upstream repository if not already added
git remote add upstream https://github.com/broadinstitute/warp.git || echo "Upstream already exists"

# Verify remote repositories
echo "Verifying remotes..."
git remote -v

# Fetch latest changes from upstream
echo "Fetching upstream changes..."
git fetch upstream

# Backup local .dockstore.yml file
echo "Backing up .dockstore.yml..."
cp .dockstore.yml .dockstore.yml.bak

# Switch to develop branch
echo "Switching to develop branch..."
git checkout develop

# Merge upstream/develop changes without auto-committing
echo "Merging upstream/develop..."
git merge upstream/develop --no-commit

# Restore local .dockstore.yml file
echo "Restoring .dockstore.yml..."
git checkout HEAD -- .dockstore.yml

# Commit the merge
echo "Committing merge..."
git commit -m "Merge upstream/develop, preserving local .dockstore.yml"

# Push changes to your fork
echo "Pushing to origin..."
git push origin develop

# Clean up backup file (optional)
echo "Cleaning up backup..."
rm .dockstore.yml.bak

echo "Sync complete! Your .dockstore.yml has been preserved."