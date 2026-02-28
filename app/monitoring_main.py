from __future__ import annotations

import uvicorn

from app.monitoring_api import create_monitoring_app

app = create_monitoring_app()


def main() -> None:
    uvicorn.run("app.monitoring_main:app", host="0.0.0.0", port=8000, reload=False)


if __name__ == "__main__":
    main()

