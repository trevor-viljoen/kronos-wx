Push the current branch and redeploy kronos-wx on Portainer.

Run: `git push && bash scripts/redeploy.sh`

The script will:
1. Wait for the GitHub Actions Docker build to pass
2. Trigger a Portainer git/redeploy on the kronos-wx stack (pulls latest images)

If there are uncommitted changes, commit them first before running /pushp.
