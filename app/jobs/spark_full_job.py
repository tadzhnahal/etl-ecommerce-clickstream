from app.services.etl_jobs import run_full_snapshot_onetl

def main() -> None:
    run_full_snapshot_onetl()

if __name__ == "__main__":
    main()