name: Series Collector — Auto-Paginate (10 items / run)

on:
  # ── Manual kick-off (first time) ──────────────────────────────────
  workflow_dispatch:
    inputs:
      force_restart:
        description: 'Clear processed-URL log and restart from the beginning?'
        required: false
        default: 'false'
        type: choice
        options:
          - 'false'
          - 'true'

  # ── Self-triggered re-runs arrive here via repository_dispatch ────
  repository_dispatch:
    types: [series_auto_continue]

# Allow the bot to push JSON changes AND trigger new workflow runs
permissions:
  contents: write
  actions: write          # needed to call the Actions API for re-dispatch

env:
  TARGET_JSON: "series.json"
  URL_LIMIT:   "10"

jobs:
  scrape-and-continue:
    runs-on: ubuntu-latest

    steps:
      # ── 1. Checkout ────────────────────────────────────────────────
      - name: Checkout Repository
        uses: actions/checkout@v4

      # ── 2. Optional hard-reset (only when manually requested) ──────
      - name: Clear processed log (force_restart only)
        if: >
          github.event_name == 'workflow_dispatch' &&
          github.event.inputs.force_restart == 'true'
        run: |
          echo "[reset] Clearing list_of_already_processed_urls.txt"
          > list_of_already_processed_urls.txt
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name  "github-actions[bot]"
          git add list_of_already_processed_urls.txt
          git diff --staged --quiet || git commit -m "Reset: cleared processed-URL log for series.json"
          git push

      # ── 3. Python setup ────────────────────────────────────────────
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install Dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests curl-cffi beautifulsoup4

      # ── 4. Count how many items are left BEFORE running ───────────
      #       If zero remain, skip scraping and skip the re-trigger.
      - name: Check remaining items
        id: check_remaining
        run: |
          python - <<'PYEOF'
          import json, os, sys

          TARGET_JSON    = "series.json"
          PROCESSED_FILE = "list_of_already_processed_urls.txt"

          if not os.path.exists(TARGET_JSON):
              print(f"[!] {TARGET_JSON} not found — nothing to do.")
              print("remaining=0")
              with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
                  fh.write("remaining=0\n")
              sys.exit(0)

          processed = set()
          if os.path.exists(PROCESSED_FILE):
              with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                  processed = {line.strip() for line in f if line.strip()}

          with open(TARGET_JSON, "r", encoding="utf-8") as f:
              data = json.load(f)

          # Series "done" check: same logic as series_already_done() in the script
          def already_done(item):
              return bool(item.get("episode-1-server1", ""))

          remaining = [
              item for item in data
              if item.get("url")
              and not already_done(item)
              and item["url"] not in processed
          ]

          print(f"[check] {len(remaining)} item(s) still need processing.")
          with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
              fh.write(f"remaining={len(remaining)}\n")
          PYEOF

      # ── 5. Run the scraper (only when items remain) ────────────────
      - name: Run Scraper
        if: steps.check_remaining.outputs.remaining != '0'
        env:
          TARGET_JSON: ${{ env.TARGET_JSON }}
          URL_LIMIT:   ${{ env.URL_LIMIT }}
        run: python server_collector_with_tmdb_imdb.py

      # ── 6. Commit & push results ───────────────────────────────────
      - name: Commit and Push Changes
        if: steps.check_remaining.outputs.remaining != '0'
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name  "github-actions[bot]"

          # Stage JSON data + both tracking files
          git add series.json \
                  list_of_already_processed_urls.txt \
                  list_of_facing_error.txt 2>/dev/null || true

          git diff --staged --quiet \
            || git commit -m "Auto-update series.json — batch of ${{ env.URL_LIMIT }} items"

          git push

      # ── 7. Count remaining items AFTER saving ─────────────────────
      - name: Count remaining after save
        if: steps.check_remaining.outputs.remaining != '0'
        id: count_after
        run: |
          python - <<'PYEOF'
          import json, os

          TARGET_JSON    = "series.json"
          PROCESSED_FILE = "list_of_already_processed_urls.txt"

          processed = set()
          if os.path.exists(PROCESSED_FILE):
              with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                  processed = {line.strip() for line in f if line.strip()}

          with open(TARGET_JSON, "r", encoding="utf-8") as f:
              data = json.load(f)

          def already_done(item):
              return bool(item.get("episode-1-server1", ""))

          remaining = [
              item for item in data
              if item.get("url")
              and not already_done(item)
              and item["url"] not in processed
          ]

          print(f"[after] {len(remaining)} item(s) still left after this batch.")
          with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
              fh.write(f"remaining_after={len(remaining)}\n")
          PYEOF

      # ── 8. Wait 5 minutes then re-trigger (if items remain) ───────
      #
      #   We use repository_dispatch instead of workflow_dispatch
      #   because the latter cannot be programmatically triggered on
      #   the default branch without a PAT when "permissions: actions"
      #   is given; repository_dispatch works with GITHUB_TOKEN.
      #
      - name: Schedule next batch in 5 minutes
        if: >
          steps.check_remaining.outputs.remaining != '0' &&
          steps.count_after.outputs.remaining_after != '0'
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          echo "[scheduler] Sleeping 5 minutes before re-triggering..."
          sleep 300

          # Fire repository_dispatch — the workflow listens on type 'series_auto_continue'
          curl -s -X POST \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GH_TOKEN}" \
            -H "X-GitHub-Api-Version: 2022-11-28" \
            "https://api.github.com/repos/${{ github.repository }}/dispatches" \
            -d '{"event_type":"series_auto_continue","client_payload":{"triggered_by":"auto"}}' \
          && echo "[scheduler] Next batch triggered successfully." \
          || echo "[scheduler] WARNING: failed to trigger next batch — check token permissions."

      # ── 9. Final status banner ─────────────────────────────────────
      - name: Print completion status
        if: always()
        run: |
          if [ "${{ steps.check_remaining.outputs.remaining }}" = "0" ]; then
            echo "======================================================"
            echo "  ✅  ALL items in series.json have been processed!"
            echo "  No further runs will be triggered automatically."
            echo "======================================================"
          elif [ "${{ steps.count_after.outputs.remaining_after }}" = "0" ]; then
            echo "======================================================"
            echo "  ✅  Last batch complete — series.json fully done!"
            echo "======================================================"
          else
            echo "------------------------------------------------------"
            echo "  ⏳  Batch finished. Next run starts in ~5 minutes."
            echo "  Remaining: ${{ steps.count_after.outputs.remaining_after }} items."
            echo "------------------------------------------------------"
          fi
