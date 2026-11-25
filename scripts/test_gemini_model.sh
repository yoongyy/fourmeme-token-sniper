#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${GEMINI_API_KEY:-}" ]]; then
  echo "GEMINI_API_KEY is not set."
  exit 1
fi

MODEL="${GEMINI_MODEL:-gemini-2.0-flash}"
PROMPT="${1:-Reply with only 'yes' if 2+2=4, otherwise 'no'.}"

curl -sS \
  "https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent?key=${GEMINI_API_KEY}" \
  -H "Content-Type: application/json" \
  -d @- <<JSON
{
  "contents": [
    {
      "parts": [
        { "text": "${PROMPT}" }
      ]
    }
  ],
  "generationConfig": {
    "temperature": 0.0,
    "maxOutputTokens": 64
  }
}
JSON

