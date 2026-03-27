from app.services.etl_jobs import run_incremental_onetl

def main() -> None:
    run_incremental_onetl()

if __name__ == "__main__":
    main()