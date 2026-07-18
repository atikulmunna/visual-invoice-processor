from pathlib import Path

from app.db_migrate import migration_files


def test_migration_files_returns_only_ordered_numbered_sql(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").write_text("select 2;", encoding="utf-8")
    (tmp_path / "000_first.sql").write_text("select 0;", encoding="utf-8")
    (tmp_path / "notes.sql").write_text("select 1;", encoding="utf-8")
    (tmp_path / "001_wrong.txt").write_text("select 1;", encoding="utf-8")

    assert [path.name for path in migration_files(tmp_path)] == [
        "000_first.sql",
        "002_second.sql",
    ]
