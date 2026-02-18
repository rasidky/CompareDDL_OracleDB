import oracledb
import argparse

#CONFIG for database

ORIGIN_USER {
    "user": "origin_user",
    "password": "origin_password",
    "dsn": "origin_db_dsn"
}

TARGET_USER {
    "user": "target_user",
    "password": "target_password",
    "dsn": "target_db_dsn"
}

#FUNCTION to get schema objects from the database
