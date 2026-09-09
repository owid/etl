"""Print each message of a saved Gmail get_thread JSON with quoted replies and signatures stripped.

Usage: python3 strip_quotes.py <saved_get_thread_result.json>
"""

import json
import re
import sys

# A line that starts the quoted part of a reply (English, French, German, Spanish, Swedish variants).
QUOTE_START = re.compile(
    r"^("
    r">"
    r"|On .{5,160} wrote:"
    r"|.*<mailto:.*>> wrote:"
    r"|From: "
    r"|-----Original Message-----"
    r"|De : |Von: |El .{5,120} escribi[oó]:|Le .{5,120} a écrit"
    r"|Den .{5,120} skrev"
    r"|\d{1,2}/\d{1,2}/\d{2,4} .*wrote:"
    r")",
    re.I,
)

# A sign-off line: the body ends here, plus at most one short name line after it.
SIGN_OFF = re.compile(
    r"^(--\s*$|kind regards|best wishes|best regards|warm regards|warm wishes|regards|best|cheers|"
    r"thanks|thank you|many thanks|thanks again|all the best|sincerely|yours|take care|"
    r"saludos|cordialement|mit freundlichen grüßen|med vänliga hälsningar)[,!. ]*$",
    re.I,
)

# Typical mobile footers and legal boilerplate that end a body when no sign-off is present.
FOOTER = re.compile(r"^(sent from my |get outlook for |this e-?mail .*confidential|caution: |varning: )", re.I)


def _join_wrapped_quote_headers(body: str) -> str:
    """Gmail wraps long headers: "On Mon, ... <a@b.c>" on one line and "wrote:" on the next. Rejoin them."""
    return re.sub(
        r"(\n(?:On|El|Le|Den|Am) [^\n]{5,200})\n[ \t]*(wrote:|escribi[oó]:|a écrit|skrev|schrieb)", r"\1 \2", body
    )


def strip(body: str) -> str:
    body = _join_wrapped_quote_headers("\n" + body)
    kept = []
    for line in body.splitlines():
        s = line.strip()
        if QUOTE_START.match(s) or FOOTER.match(s):
            break
        kept.append(line.rstrip())
    lines = "\n".join(kept).strip().splitlines()
    for i, line in enumerate(lines):
        if i > 0 and SIGN_OFF.match(line.strip()):
            tail = [line]
            # Keep a following short name line ("Nic", "Dr. J. Smith"), drop titles, orgs and links.
            for nxt in lines[i + 1 :]:
                if not nxt.strip():
                    continue
                if len(nxt.split()) <= 4 and not re.search(r"[@|]|https?://|www\.", nxt):
                    tail.append(nxt)
                break
            lines = lines[:i] + tail
            break
    text = "\n".join(lines).strip()
    return re.sub(r"\n{3,}", "\n\n", text)


def main(path: str) -> None:
    data = json.load(open(path))
    for m in data["messages"]:
        print("=" * 80)
        print(
            f"id={m['id']} date={m['date'][:10]} from={m['sender']} "
            f"to={','.join(m.get('toRecipients', []))} cc={','.join(m.get('ccRecipients', []))}"
        )
        print(f"subject={m.get('subject', '')}")
        print("-" * 80)
        print(strip(m.get("plaintextBody", "")))


if __name__ == "__main__":
    main(sys.argv[1])
