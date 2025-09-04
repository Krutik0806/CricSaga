from db_handler import DatabaseHandler
from constants import DB_CONFIG, DB_POOL_MIN, DB_POOL_MAX

db = DatabaseHandler(DB_CONFIG, minconn=DB_POOL_MIN, maxconn=DB_POOL_MAX)