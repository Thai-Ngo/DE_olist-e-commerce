import psycopg2 
from datetime import datetime
import glob 
import DE_user_query

conn = psycopg2.connect(database="my_pgdb", 
                        user='postgres', password='1234',  
                        host='127.0.0.1', port='5432'
) 

conn.autocommit = True
cursor = conn.cursor() 

def init_database(cursor):
    DE_user_query.COMM_for_all_database(cursor, "DROP")
    DE_user_query.COMM_for_all_database(cursor, "CREATE")
    DE_user_query.COMM_for_all_database(cursor, "TRUNCATE")
    DE_user_query.COMM_for_all_database(cursor, "COPY")
    
    
def init_tmp_database(cursor):
    for i in range(len(DE_user_query.CREATE_TMP_table_comm_list)):
        cursor.execute(DE_user_query.CREATE_TMP_table_comm_list[i])
        
def update_to_database(cursor):
    for i in range(len(DE_user_query.CREATE_TMP_table_comm_list)):
        print(DE_user_query.COPY_TMP_table_comm_list[i])
        cursor.execute(DE_user_query.COPY_TMP_table_comm_list[i])
        cursor.execute(DE_user_query.TRUNCATE_n_INSERT_INTO_table_comm_list[i])
        cursor.execute(DE_user_query.TRUNCATE_TMP_table_comm_list[i])

if __name__ == '__main__':
    # init_database(cursor)
    # init_tmp_database(cursor)
    update_to_database(cursor)

    conn.commit() 
    conn.close() 