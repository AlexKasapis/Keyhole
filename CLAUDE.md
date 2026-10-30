# Definition of Done
- Changes must be accompanied with tests.
- Each task is complete when it functions, and covered and linted as defined in the `/scripts/test.sh` and `/scripts/lint.sh`.

# Workflow
- All work is done directly on `main` in this checkout. Do not create branches or worktrees.
- Fetch and fast-forward `main` from `origin` before starting a task; Dependabot merges land on GitHub.
- Ignore modified code in the checkout that is not yours. Do not change, commit or stash it.
- At the end of each task, commit your changes to `main` and push.

# Reasoning
- If something is ambiguous, ask questions.