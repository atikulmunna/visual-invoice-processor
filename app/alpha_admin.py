from __future__ import annotations

import argparse
import getpass

from app.alpha_store import AlphaStore, generate_password
from app.config import load_dotenv


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Manage private-alpha tester accounts")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create")
    create.add_argument("--username", required=True)
    create.add_argument("--password")
    create.add_argument("--document-limit", type=int, default=20)

    for name in ("enable", "disable", "reset"):
        command = sub.add_parser(name)
        command.add_argument("--username", required=True)
    sub.add_parser("list")

    args = parser.parse_args()
    store = AlphaStore.from_env()
    if args.command == "create":
        password = args.password or generate_password()
        if args.password is None and not password:
            password = getpass.getpass("Password: ")
        user = store.create_user(
            args.username,
            password,
            document_limit=args.document_limit,
            max_users=10,
        )
        print(f"Created {user.username}; temporary password: {password}")
    elif args.command == "enable":
        store.set_user_active(args.username, True)
    elif args.command == "disable":
        store.set_user_active(args.username, False)
    elif args.command == "reset":
        store.reset_user_usage(args.username)
    else:
        for user in store.list_users():
            print(
                f"{user.username}: active={user.is_active} "
                f"used={user.documents_used}/{user.document_limit}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

