from __future__ import annotations

import argparse
import getpass

from app.alpha_store import ORG_ROLES, AlphaStore, generate_password
from app.config import load_dotenv


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Manage private-alpha tester accounts and organizations")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create")
    create.add_argument("--username", required=True)
    create.add_argument("--password")
    create.add_argument("--document-limit", type=int, default=20)

    for name in ("enable", "disable", "reset"):
        command = sub.add_parser(name)
        command.add_argument("--username", required=True)
    sub.add_parser("list")

    org_create = sub.add_parser("org-create", help="Create an organization owned by an existing tester")
    org_create.add_argument("--name", required=True)
    org_create.add_argument("--owner", required=True, help="Username of the owner")

    org_add = sub.add_parser("org-add-member", help="Add a tester to an organization, or change their role")
    org_add.add_argument("--org-id", required=True)
    org_add.add_argument("--username", required=True)
    org_add.add_argument("--role", choices=ORG_ROLES, default="member")

    org_remove = sub.add_parser("org-remove-member", help="Remove a tester from an organization")
    org_remove.add_argument("--org-id", required=True)
    org_remove.add_argument("--username", required=True)

    sub.add_parser("org-list", help="List organizations and their members")

    adopt = sub.add_parser(
        "org-adopt-unowned",
        help="Assign ledger records and review items that have no organization to one",
    )
    adopt.add_argument("--org-id", required=True)

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
        print(f"Created {user.username} in organization {user.org_id}; temporary password: {password}")
    elif args.command == "enable":
        store.set_user_active(args.username, True)
    elif args.command == "disable":
        store.set_user_active(args.username, False)
    elif args.command == "reset":
        store.reset_user_usage(args.username)
    elif args.command == "org-create":
        org = store.create_organization(args.name, owner_username=args.owner)
        print(f"Created organization {org.name} ({org.id}) owned by {args.owner}")
    elif args.command == "org-add-member":
        store.add_member(args.org_id, args.username, role=args.role)
        print(f"{args.username} is now {args.role} of {args.org_id}")
    elif args.command == "org-remove-member":
        store.remove_member(args.org_id, args.username)
        print(f"Removed {args.username} from {args.org_id}")
    elif args.command == "org-list":
        for org in store.list_organizations():
            members = ", ".join(f"{username} ({role})" for username, role in org.members) or "no members"
            print(f"{org.id}  {org.name}: {members}")
    elif args.command == "org-adopt-unowned":
        counts = store.adopt_unowned_records(args.org_id)
        print(
            f"Assigned {counts['ledger_records']} ledger records and "
            f"{counts['review_items']} review items to {args.org_id}"
        )
    else:
        for user in store.list_users():
            print(
                f"{user.username}: active={user.is_active} "
                f"used={user.documents_used}/{user.document_limit}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
