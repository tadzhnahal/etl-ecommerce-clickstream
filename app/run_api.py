import uvicorn
from app.core.config import config

def main() -> None:
    api_config = config["api"]

    uvicorn.run(
        "app.main:app",
        host=api_config["host"],
        port=int(api_config["port"]),
        reload=True,
    )

if __name__ == "__main__":
    main()