import os

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        """python-dotenv가 없는 환경에서는 이미 주입된 환경 변수만 사용한다."""
        return False


def require_database_url():
    """worker가 접속할 PostgreSQL DATABASE_URL을 환경 변수에서 읽는다."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError(
            "DATABASE_URL이 필요합니다. trader-data/.env 또는 셸 환경 변수에 설정하세요."
        )
    return value


def connect_database():
    """trader-data/.env를 로드한 뒤 psycopg2 PostgreSQL 연결을 생성한다."""
    load_dotenv()
    import psycopg2

    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    return connection
