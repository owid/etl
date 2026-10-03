#!/usr/bin/env python3
"""
Fetch DOD (Definition of Data) content from the database.
This script is called by the VS Code extension to get DOD definitions.
"""

import json
import os
import sys
from pathlib import Path

# Repo root: .../etl (the script lives at .../etl/vscode_extensions/dod-syntax/scripts/fetch_dod.py)
repo_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(repo_root))

# Ensure we can import from ETL

try:
    from dotenv import dotenv_values

    from etl.config import OWID_ENV, ENV_FILE_PROD, OWIDEnv

    def dod_env_candidates() -> list[tuple[OWIDEnv, str]]:
        """Return ordered (env, source_label) candidates for DoD lookups.

        DoDs are edited on staging first and only synced to production
        periodically, so a developer working on a staging branch should see
        their own staging DoDs — including edits not yet on live_grapher.
        Users off staging fall back to production.

        Priority:
        1. `OWID_ENV` when `STAGING` is set — the developer's staging server,
           where in-flight DoD edits live.
        2. ENV_FILE_PROD (explicit prod env file).
        3. `.env.prod` in the repo root, by convention.
        4. The repo's `.env`, read raw — only if DB_NAME=live_grapher.
           Reading raw bypasses the STAGING override applied to OWID_ENV.
        5. `OWID_ENV` as final fallback (local dev DB, etc.).

        The caller tries candidates in order and falls through on connection
        errors. The source label is surfaced to the extension so the user can
        tell where the hover content came from — we show it in the hover for
        anything other than production.
        """
        candidates: list[tuple[OWIDEnv, str]] = []
        owid_conf = OWID_ENV.conf

        # 1. Staging — the working copy for DoD edits.
        if os.environ.get("STAGING") and owid_conf.DB_NAME == "owid":
            candidates.append((OWID_ENV, f"staging ({owid_conf.DB_HOST})"))

        # 2. ENV_FILE_PROD
        if ENV_FILE_PROD and Path(ENV_FILE_PROD).exists():
            candidates.append((OWIDEnv.from_env_file(ENV_FILE_PROD), "production"))

        # 3. .env.prod
        env_prod = repo_root / ".env.prod"
        if env_prod.exists():
            candidates.append((OWIDEnv.from_env_file(str(env_prod)), "production"))

        # 4. .env raw, only if it targets live_grapher
        env_main = repo_root / ".env"
        if env_main.exists() and dotenv_values(env_main).get("DB_NAME") == "live_grapher":
            candidates.append((OWIDEnv.from_env_file(str(env_main)), "production"))

        # 5. OWID_ENV fallback (local dev DB, etc.) unless already queued above.
        if not any(c.conf == owid_conf for c, _ in candidates):
            candidates.append((OWID_ENV, f"{owid_conf.DB_NAME}@{owid_conf.DB_HOST}"))

        return candidates

    def _try_query(query: str, params: tuple | None = None):
        """Run `query` against the first reachable candidate env. Returns
        `(dataframe, source_label)`. Raises the last OperationalError if none
        of the candidates is reachable."""
        from sqlalchemy.exc import OperationalError

        last_error: Exception | None = None
        for env, source in dod_env_candidates():
            try:
                if params is None:
                    df = env.read_sql(query)
                else:
                    df = env.read_sql(query, params=params)
                return df, source
            except OperationalError as e:
                last_error = e
                continue
        assert last_error is not None, "dod_env_candidates() returned no candidates"
        raise last_error

    def fetch_dod_by_names(dod_names: list[str]) -> dict:
        """
        Fetch DOD definitions by names from the database.

        Args:
            dod_names: List of DOD names (after #dod:)

        Returns:
            Dictionary with DOD data or error information for each key
        """
        try:
            if not dod_names:
                return {"success": True, "source": "production", "dods": {}}

            # Create placeholders for the IN clause
            placeholders = ", ".join(["%s"] * len(dod_names))
            query = f"""
                SELECT
                    d.id,
                    d.name,
                    d.content,
                    d.createdAt,
                    d.updatedAt,
                    u.fullName as lastUpdatedBy
                FROM dods d
                LEFT JOIN users u ON d.lastUpdatedUserId = u.id
                WHERE d.name IN ({placeholders})
            """

            df, source = _try_query(query, params=tuple(dod_names))

            # Build result dictionary
            result = {"success": True, "source": source, "dods": {}}
            found_names = set()

            for _, row in df.iterrows():
                dod_name = row["name"]
                found_names.add(dod_name)
                result["dods"][dod_name] = {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "content": row["content"],
                    "createdAt": str(row["createdAt"]) if row["createdAt"] else None,
                    "updatedAt": str(row["updatedAt"]) if row["updatedAt"] else None,
                    "lastUpdatedBy": row["lastUpdatedBy"] if row["lastUpdatedBy"] else None,
                    "source": source,
                }

            # Add entries for DODs that weren't found
            for dod_name in dod_names:
                if dod_name not in found_names:
                    result["dods"][dod_name] = {
                        "success": False,
                        "error": f"DOD '{dod_name}' not found in database",
                        "dod_name": dod_name,
                        "source": source,
                    }

            return result

        except Exception as e:
            return {
                "success": False,
                "error": f"Database error: {str(e)}",
                "dod_names": dod_names,
                "details": "Make sure you have access to the production database and your .env.prod file is configured",
            }

    def fetch_all_dod_names() -> dict:
        """
        Fetch all DOD names from the database for autocomplete.

        Returns:
            Dictionary with list of all DOD names or error information
        """
        try:
            query = """
                SELECT DISTINCT d.name
                FROM dods d
                ORDER BY d.name
            """

            df, source = _try_query(query)

            if df.empty:
                return {"success": True, "source": source, "dod_names": []}

            dod_names = df["name"].tolist()
            return {"success": True, "source": source, "dod_names": dod_names}

        except Exception as e:
            return {
                "success": False,
                "error": f"Database error: {str(e)}",
                "details": "Make sure you have access to the production database and your .env.prod file is configured",
            }

    def main():
        if len(sys.argv) < 2:
            print(
                json.dumps(
                    {
                        "success": False,
                        "error": "Usage: python fetch_dod.py <command> [args...]\nCommands: --names (get all names), <dod_name1> [dod_name2] ... (get specific DODs)",
                    }
                )
            )
            sys.exit(1)

        if sys.argv[1] == "--names":
            # Fetch all DOD names for autocomplete
            result = fetch_all_dod_names()
        else:
            # Fetch specific DOD definitions
            dod_names = sys.argv[1:]
            result = fetch_dod_by_names(dod_names)

        print(json.dumps(result))

    if __name__ == "__main__":
        main()

except ImportError as e:
    # Handle case where ETL dependencies are not available
    print(
        json.dumps(
            {
                "success": False,
                "error": f"ETL import error: {str(e)}. Make sure you're in the ETL repository.",
                "dod_name": sys.argv[1] if len(sys.argv) > 1 else "unknown",
            }
        )
    )
    sys.exit(1)
except Exception as e:
    print(
        json.dumps(
            {
                "success": False,
                "error": f"Unexpected error: {str(e)}",
                "dod_name": sys.argv[1] if len(sys.argv) > 1 else "unknown",
            }
        )
    )
    sys.exit(1)
