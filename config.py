import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(self) -> None:
        self.db_driver: str = os.getenv(
            "DB_DRIVER", "ODBC Driver 17 for SQL Server"
        )
        self.db_server: str = os.getenv("DB_SERVER", "LAPTOP-OVEBT7VJ")
        self.db_database: str = os.getenv("DB_DATABASE", "ShaunTools")
        self.db_username: str = os.getenv("DB_USERNAME", "") or None
        self.db_password: str = os.getenv("DB_PASSWORD", "") or None
        self.db_trusted_connection: bool = (
            os.getenv("DB_TRUSTED_CONNECTION", "yes").strip().lower() in ("1", "true", "yes")
        )

    @property
    def connection_string(self) -> str:
        parts = [
            f"DRIVER={{{self.db_driver}}}",
            f"SERVER={self.db_server}",
            f"DATABASE={self.db_database}",
        ]
        if self.db_trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self.db_username}")
            parts.append(f"PWD={self.db_password}")
        return ";".join(parts)


settings = Settings()
