import os
import psycopg2
from psycopg2.extras import DictCursor


target_datasets = [
    "S2B_MSIL1C_20240915T102559_N0511_R108_T33WXQ_20240915T123129",
    "S2B_MSIL1C_20240915T102559_N0511_R108_T33VVH_20240915T123129",
    "S2B_MSIL1C_20240915T102559_N0511_R108_T33VVJ_20240915T123129",
    "S2B_MSIL1C_20240915T102559_N0511_R108_T33VWJ_20240915T123129",
    "S2B_MSIL1C_20240915T102559_N0511_R108_T33VWH_20240915T123129",
    "S2B_MSIL1C_20230819T101609_N0509_R065_T33VUC_20230819T123928",
    "S2A_MSIL1C_20230708T102601_N0509_R108_T33VUC_20230708T141205",
    "S2A_MSIL1C_20220720T101611_N0400_R065_T33VUC_20220720T140828",
    "S2B_MSIL1C_20240915T102559_N0511_R108_T33WXR_20240915T123129",
    "S2B_MSIL1C_20230819T101609_N0509_R065_T33VVC_20230819T123928",
    "S2B_MSIL1C_20230819T101609_N0509_R065_T33VVD_20230819T123928",
    "S2B_MSIL1C_20230819T101609_N0509_R065_T33VUD_20230819T123928",
    "S2A_MSIL1C_20230708T102601_N0509_R108_T33VVC_20230708T141205",
    "S2A_MSIL1C_20230708T102601_N0509_R108_T33VVD_20230708T141205",
    "S2A_MSIL1C_20230708T102601_N0509_R108_T33VUD_20230708T141205",
    "S2A_MSIL1C_20220720T101611_N0400_R065_T33VVC_20220720T140828",
    "S2A_MSIL1C_20220720T101611_N0400_R065_T33VVD_20220720T140828",
    "S2A_MSIL1C_20220720T101611_N0400_R065_T33VUD_20220720T140828",
]


def insert_test_data(target_datasets):
    # Database connection details
    db_host = os.getenv("BNP_DB_HOSTNAME", "datasource.main.rise-ck8s.com")
    db_port = os.getenv("BNP_DB_PORT", 30103)
    db_user = os.getenv("BNP_DB_USERNAME", "bnp_db_rw")
    db_password = os.getenv("BNP_DB_PASSWORD", "bnp_password")
    db_name = os.getenv("BNP_DB_DATABASE", "datacube")

    # Validate password
    if not db_password:
        raise ValueError("BNP_DB_PASSWORD is not set in the environment variables")

    # Establish the database connection
    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name,
        cursor_factory=DictCursor,
    )

    try:
        with connection.cursor() as cursor:
            for dataset in target_datasets:
                print("Looking for ", dataset)
                # Find the matching row in agdc.dataset_location
                cursor.execute(
                    """
                    SELECT id, dataset_ref, uri_scheme, uri_body, added
                    FROM agdc.dataset_location
                    WHERE uri_body LIKE %s
                    """,
                    (f"%{dataset}%",),
                )
                row = cursor.fetchone()
                print(row)
                # If a matching row is found, insert it into bnp.dataset_location
                if row:
                    cursor.execute(
                        """
                        INSERT INTO bnp.dataset_location (id, dataset_ref, uri_scheme, uri_body, added)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (
                            row["id"],
                            row["dataset_ref"],
                            row["uri_scheme"],
                            row["uri_body"],
                            row["added"],
                        ),
                    )
                    print(f"Inserted {row['uri_body']} into bnp.dataset_location")
                else:
                    print(f"No matching dataset found for {dataset}")

        # Commit the transaction
        connection.commit()
        print("All datasets inserted successfully.")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()

    finally:
        connection.close()


# Example usage
insert_test_data(target_datasets)
