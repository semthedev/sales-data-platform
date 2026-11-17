from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Maven coordinates resolved by Spark when no local driver jars are configured.
DEFAULT_SPARK_PACKAGES = "org.postgresql:postgresql:42.7.14,com.clickhouse:clickhouse-jdbc:0.9.8"


@dataclass(frozen=True, slots=True)
class Settings:
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "sales"
    postgres_user: str = "sales"
    postgres_password: str = "sales"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    spark_master: str = "local[*]"
    spark_jars: str = ""
    spark_packages: str = DEFAULT_SPARK_PACKAGES
    project_root: Path = Path(__file__).resolve().parents[1]

    @classmethod
    def from_environment(cls) -> Settings:
        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "sales"),
            postgres_user=os.getenv("POSTGRES_USER", "sales"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "sales"),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            spark_jars=os.getenv("SPARK_JARS", ""),
            spark_packages=os.getenv("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES),
        )

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def clickhouse_jdbc_url(self) -> str:
        return f"jdbc:clickhouse://{self.clickhouse_host}:{self.clickhouse_port}/sales"
