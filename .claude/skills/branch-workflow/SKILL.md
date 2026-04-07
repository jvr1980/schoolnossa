---
name: branch-workflow
description: "Git branching workflow — creates feature branches for changes, merges to main after user confirms testing. Use when the user asks to make code changes, implement features, or fix bugs. Also trigger proactively when starting ANY implementation task to ensure a feature branch is created first, when a significant unit of work is complete and should be committed, or when the user says 'commit', 'push', 'merge', 'save progress', 'let's wrap up', or 'branch'. Provides safe rollback via merge commits."
allowed-tools: Bash, Read, Edit, Write, Glob, Grep, Agent
---

# Git Branch Workflow

Follow this workflow whenever making code changes.

## Phase 1: Start a Feature Branch

Before making ANY code changes:

1. Check current state:
   ```bash
   git status
   git branch --show-current
   ```

2. If already on a feature branch from a previous task, that's fine — continue on it. Otherwise:
   - Switch to main and pull latest:
     ```bash
     git checkout main
     git pull origin main
     ```
   - Create a feature branch with a short descriptive name:
     ```bash
     git checkout -b feature/<short-description>
     ```
     Examples: `feature/add-macro-download`, `feature/fix-nan-handling`, `feature/update-dashboard-ui`

3. Tell the user which branch you created.

## Phase 2: Make Changes

1. Implement the requested changes on the feature branch.
2. Commit regularly with meaningful messages as work progresses.
3. Each commit message should end with:
   ```
   Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
   ```
4. Push the branch to remote periodically for backup:
   ```bash
   git push -u origin feature/<branch-name>
   ```
5. **Update the Development Journal** (`docs/DEVJOURNAL.md`):
   - Add a new dated entry ABOVE the `<!-- NEW ENTRIES GO ABOVE THIS LINE -->` marker
   - Use the format: `## YYYY-MM-DD — Short Title`
   - Include **What** (what was done), **Why** (the reasoning/problem), and key **Results** or decisions
   - Be specific: mention file names, numbers, technical details — this is a reference for the future
   - Group related changes under a single date heading with sub-headings (`###`)
   - If continuing work on the same day as an existing entry, add to that entry rather than creating a duplicate

## Phase 3: Confirm & Merge

When the implementation is complete:

1. **Ask the user to test.** Say something like:
   > "The changes are ready on branch `feature/<name>`. Please test them. When you're satisfied, let me know and I'll merge to main. If something needs fixing, just tell me."

2. **Wait for explicit confirmation.** Do NOT merge until the user says they are happy.

3. **On user confirmation — merge to main:**
   ```bash
   git checkout main
   git pull origin main
   git merge feature/<branch-name> --no-ff -m "Merge feature/<branch-name>: <brief description>"
   git push origin main
   ```

4. **Clean up the feature branch:**
   ```bash
   git branch -d feature/<branch-name>
   git push origin --delete feature/<branch-name>
   ```

5. Confirm to the user: "Merged to main and cleaned up the feature branch."

## Rollback

If the user wants to undo a merged change:

1. Find the merge commit:
   ```bash
   git log --oneline --merges -10
   ```

2. Revert it (keeps history, safe to do):
   ```bash
   git revert -m 1 <merge-commit-hash>
   git push origin main
   ```

3. This cleanly undoes the entire feature while preserving history.

If the user wants to see what changed in a specific merge:
```bash
git log --oneline --graph -20
git diff <merge-commit-hash>^..<merge-commit-hash>
```

## Rules

- NEVER make code changes directly on `main`
- NEVER force push to `main`
- ALWAYS use `--no-ff` merges so each feature is a single revertable commit
- ALWAYS ask the user to confirm before merging
- If the user asks to "roll back" or "undo", use `git revert` — NEVER `git reset --hard`
