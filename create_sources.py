from source_manager import ensure_sources_file


def main() -> None:
    file_path = ensure_sources_file("sources.xlsx")
    print(f"Source file ready: {file_path.resolve()}")


if __name__ == "__main__":
    main()
