"""Administrator command line for tester accounts, organizations, and vendor maintenance.

Each command is a small function registered with its arguments; main parses and runs one.
"""

from __future__ import annotations

import argparse
import os
from typing import Callable

from app.alpha_store import ORG_ROLES, AlphaStore, generate_password
from app.config import load_dotenv
from app.vendor_store import link_unlinked_records, refresh_alias_forms

Command = Callable[[AlphaStore, argparse.Namespace], None]


def _create(store: AlphaStore, args: argparse.Namespace) -> None:
    password = args.password or generate_password()
    user = store.create_user(args.username, password, document_limit=args.document_limit, max_users=10)
    print(f"Created {user.username} in organization {user.org_id}; temporary password: {password}")


def _enable(store: AlphaStore, args: argparse.Namespace) -> None:
    store.set_user_active(args.username, True)


def _disable(store: AlphaStore, args: argparse.Namespace) -> None:
    store.set_user_active(args.username, False)


def _reset(store: AlphaStore, args: argparse.Namespace) -> None:
    store.reset_user_usage(args.username)


def _list(store: AlphaStore, args: argparse.Namespace) -> None:
    for user in store.list_users():
        print(f"{user.username}: active={user.is_active} used={user.documents_used}/{user.document_limit}")


def _set_password(store: AlphaStore, args: argparse.Namespace) -> None:
    password = args.password or generate_password()
    store.set_password(args.username, password)
    print(f"New password for {args.username} (shown once): {password}")


def _org_create(store: AlphaStore, args: argparse.Namespace) -> None:
    org = store.create_organization(args.name, owner_username=args.owner)
    print(f"Created organization {org.name} ({org.id}) owned by {args.owner}")


def _org_add_member(store: AlphaStore, args: argparse.Namespace) -> None:
    store.add_member(args.org_id, args.username, role=args.role)
    print(f"{args.username} is now {args.role} of {args.org_id}")


def _org_remove_member(store: AlphaStore, args: argparse.Namespace) -> None:
    store.remove_member(args.org_id, args.username)
    print(f"Removed {args.username} from {args.org_id}")


def _org_list(store: AlphaStore, args: argparse.Namespace) -> None:
    for org in store.list_organizations():
        members = ", ".join(f"{username} ({role})" for username, role in org.members) or "no members"
        print(f"{org.id}  {org.name} [{org.base_currency}]: {members}")


def _org_set_currency(store: AlphaStore, args: argparse.Namespace) -> None:
    code = store.set_base_currency(args.org_id, args.currency)
    print(f"Base currency of {args.org_id} is now {code}")


def _org_adopt_unowned(store: AlphaStore, args: argparse.Namespace) -> None:
    counts = store.adopt_unowned_records(args.org_id)
    print(f"Assigned {counts['ledger_records']} ledger records and {counts['review_items']} review items to {args.org_id}")


def _vendors_link(store: AlphaStore, args: argparse.Namespace) -> None:
    print(f"Linked {link_unlinked_records(os.environ.get('POSTGRES_DSN', ''))} records to vendors")


def _vendors_refresh_aliases(store: AlphaStore, args: argparse.Namespace) -> None:
    result = refresh_alias_forms(os.environ.get("POSTGRES_DSN", ""))
    print(f"Added {result['added']} alias forms; {result['conflicts']} belong to another vendor (merge them if they match)")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage private-alpha tester accounts and organizations")
    sub = parser.add_subparsers(dest="command", required=True)

    def command(name: str, run: Command, help_text: str | None = None) -> argparse.ArgumentParser:
        parsed = sub.add_parser(name, help=help_text)
        parsed.set_defaults(run=run)
        return parsed

    create = command("create", _create)
    create.add_argument("--username", required=True)
    create.add_argument("--password")
    create.add_argument("--document-limit", type=int, default=20)
    for name, run in (("enable", _enable), ("disable", _disable), ("reset", _reset)):
        command(name, run).add_argument("--username", required=True)
    command("list", _list)

    set_password = command("set-password", _set_password,
                           "Give a tester a new password (generated unless --password is given) and end their sessions")
    set_password.add_argument("--username", required=True)
    set_password.add_argument("--password")

    org_create = command("org-create", _org_create, "Create an organization owned by an existing tester")
    org_create.add_argument("--name", required=True)
    org_create.add_argument("--owner", required=True, help="Username of the owner")

    org_add = command("org-add-member", _org_add_member, "Add a tester to an organization, or change their role")
    org_add.add_argument("--org-id", required=True)
    org_add.add_argument("--username", required=True)
    org_add.add_argument("--role", choices=ORG_ROLES, default="member")

    org_remove = command("org-remove-member", _org_remove_member, "Remove a tester from an organization")
    org_remove.add_argument("--org-id", required=True)
    org_remove.add_argument("--username", required=True)

    command("org-list", _org_list, "List organizations and their members")

    org_currency = command("org-set-currency", _org_set_currency,
                           "Set the base currency used when a document shows no readable currency")
    org_currency.add_argument("--org-id", required=True)
    org_currency.add_argument("--currency", required=True, help="Three-letter ISO code, such as BDT or USD")

    command("org-adopt-unowned", _org_adopt_unowned,
            "Assign ledger records and review items that have no organization to one").add_argument(
        "--org-id", required=True)

    command("vendors-link", _vendors_link, "Link stored records that have no vendor yet (run once after migration 007)")
    command("vendors-refresh-aliases", _vendors_refresh_aliases,
            "Add stored vendor names in the current matching form (run after the matching rules change)")
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parser().parse_args(argv)
    args.run(AlphaStore.from_env(), args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
