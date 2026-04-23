#!/usr/bin/env python3
"""
Fetch DOD (Definition of Data) content from the database.
This script is called by the VS Code extension to get DOD definitions.
"""

import json
import sys
from pathlib import Path

# Repo root: .../etl (the script lives at .../etl/vscode_extensions/dod-syntax/scripts/fetch_dod.py)
repo_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(repo_root))

# Ensure we can import from ETL

try:
    from dotenv import dotenv_values

    from etl.config import ENV_FILE_PROD, OWIDEnv

    def get_prod_env() -> OWIDEnv:
        """Return an OWIDEnv connected to the production live_grapher DB.

        Priority:
        1. ENV_FILE_PROD (explicit), if configured and present.
        2. `.env.prod` in the repo root, by convention.
        3. The repo's `.env`, read raw via dotenv_values — used only if it
           targets `live_grapher` (DB_NAME=live_grapher). Reading raw bypasses
           any STAGING override that `etl.config` may have applied to
           `OWID_ENV`, which would otherwise point the DoD query at a staging
           server instead of production.

        Refuses to silently fall back to a non-prod env so DoD hover never
        queries the wrong database.
        """
        if ENV_FILE_PROD and Path(ENV_FILE_PROD).exists():
            return OWIDEnv.from_env_file(ENV_FILE_PROD)

        env_prod = repo_root / ".env.prod"
        if env_prod.exists():
            return OWIDEnv.from_env_file(str(env_prod))

        env_main = repo_root / ".env"
        if env_main.exists() and dotenv_values(env_main).get("DB_NAME") == "live_grapher":
            return OWIDEnv.from_env_file(str(env_main))

        raise RuntimeError(
            "No production DB configured: set ENV_FILE_PROD, create .env.prod, "
            "or point .env at live_grapher."
        )

    def fetch_dod_by_names(dod_names: list[str]) -> dict:
        """
        Fetch DOD definitions by names from the database.

        Args:
            dod_names: List of DOD names (after #dod:)

        Returns:
            Dictionary with DOD data or error information for each key
        """
        try:
            prod_env = get_prod_env()

            if not dod_names:
                return {"success": True, "dods": {}}

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

            df = prod_env.read_sql(query, params=tuple(dod_names))

            # Build result dictionary
            result = {"success": True, "dods": {}}
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
                }

            # Add entries for DODs that weren't found
            for dod_name in dod_names:
                if dod_name not in found_names:
                    result["dods"][dod_name] = {
                        "success": False,
                        "error": f"DOD '{dod_name}' not found in database",
                        "dod_name": dod_name,
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
            prod_env = get_prod_env()

            query = """
                SELECT DISTINCT d.name
                FROM dods d
                ORDER BY d.name
            """

            df = prod_env.read_sql(query)

            if df.empty:
                return {"success": True, "dod_names": []}

            dod_names = df["name"].tolist()
            return {"success": True, "dod_names": dod_names}

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
