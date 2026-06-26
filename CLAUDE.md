# Definition of Done
- Changes must be accompanied with tests.
- Each task is complete when it functions, and covered and linted as defined in the `/scripts/test.sh` and `/scripts/lint.sh`.

# Workflow
- All working changes must take place inside a git worktree, unique to this session.
- Worktrees should be placed inside the repo, in `/.wt/`
- Ignore modified code in your worktree that is not yours. Do not change, commit or stash it.
- At the end of each task, commit your changes to main and delete your worktree.
- Follow up changes must use new, unique worktrees.

# Reasoning
- If something is ambiguous, ask questions.