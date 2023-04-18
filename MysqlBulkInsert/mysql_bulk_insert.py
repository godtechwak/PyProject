import asyncio
import aiomysql
import pymysql
from time import ctime
import os
import random
import sys
from datetime import datetime
import conn_info as config

class BulkInsert:
    db_config = {
        'host': config.DB_CONNECTION_INFO['ENDPOINT'],
        'port': config.DB_CONNECTION_INFO['PORT'],
        'user': config.DB_CONNECTION_INFO['USER'],
        'password': config.DB_CONNECTION_INFO['PASSWD'],
        'db': config.DB_CONNECTION_INFO['DBNAME']
    }

    def __init__(self):
        self.start_time=datetime.now()
        print('\n𝐒𝐓𝐀𝐑𝐓 𝐓𝐈𝐌𝐄: ', self.start_time.isoformat(timespec='milliseconds'))

    def cr_dr_tr_table(self, tbl_work_num, work_iter_num):
        before_tbl_count=work_iter_num

        conn=pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        passwd=db_config['password'],
        db=db_config['db'],
        port=db_config['port']
        )

        cursor=conn.cursor()

        for i in range(work_iter_num):
            if (tbl_work_num == 100):
                query=f"CREATE TABLE test.test{i} (id bigint auto_increment primary key, seq_1 bigint, seq_2 bigint, seq_3 bigint, name varchar(10), remark varchar(100), created_at datetime, updated_at datetime, key ix_seq1_seq2(seq_1, seq_2), key ix_seq3(seq_3), key ix_seq1_createdat(seq_1, created_at));\n"
            elif (tbl_work_num == 200):
                query=f"DROP TABLE test.test{i};\n"
            elif (tbl_work_num == 300):
                query="TRUNCATE TABLE test.test{i};\n"

            try:
                cursor.execute(query)
                conn.commit()
                print(f"𝚂𝚄𝙲𝙲𝙴𝚂𝚂!!! test{i}")
            except:
                print(f"FAIL!!(ErrorTableNo: test{i})")
                conn.rollback()

    async def async_io(self, table_num, tbl_work_num):
        try:
            iteration_num = int(input("- 𝖭𝖴𝖬𝖡𝖤𝖱 𝖮𝖥 𝖨𝖳𝖤𝖱𝖠𝖳𝖨𝖮𝖭𝖲(𝖣𝖤𝖥𝖠𝖴𝖫𝖳=𝟣): ") or "1")
            min_thread_num = int(input("- 𝖬 𝖨𝖭𝖨𝖬 𝖴𝖬 𝖳𝖧𝖱𝖤𝖠𝖣 𝖯𝖮𝖮𝖫(𝖣𝖤𝖥𝖠𝖴𝖫𝖳=𝟧𝟢): ") or "50")
            max_thread_num = int(input("- 𝖬 𝖠𝖷𝖨𝖬 𝖴𝖬 𝖳𝖧𝖱𝖤𝖠𝖣 𝖯𝖮𝖮𝖫(𝖣𝖤𝖥𝖠𝖴𝖫𝖳=𝟧𝟢): ") or min_thread_num)
        except:
            print("𝐄𝐗𝐈𝐓..")

        self.start_time=datetime.now()
        print('\n𝐒𝐓𝐀𝐑𝐓 𝐓𝐈𝐌𝐄: ', self.start_time.isoformat(timespec='milliseconds'))

        # create connection pool
        pool = await aiomysql.create_pool(minsize=min_thread_num, maxsize=max_thread_num, **db_config)

        async def execute_query(self, pool, table_num, tbl_work_num):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        rnd_num=random.randrange(0, table_num)
                        if (tbl_work_num == 1): ## INSERT
                            await cur.execute(f"INSERT INTO test{rnd_num} (id, seq_1, seq_2, seq_3, name, remark, created_at, updated_at) VALUES (null, floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), left(uuid(), 4), concat('test', floor(rand()*10)), now(), now())")
                            await conn.commit()
                        if (tbl_work_num == 2): ## DELETE
                            await cur.execute(f"DELETE FROM test{rnd_num} IN (floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000)) AND seq_2 BETWEEN floor(rand()*1000000/2) AND floor(rand()*1000000)")
                            await conn.commit()
                        if (tbl_work_num == 3): ## UPDATE
                            await cur.execute(f"UPDATE test{rnd_num} SET seq_1=floor(rand()*1000000), seq_2=floor(rand()*1000000), seq_3=floor(rand()*1000000) WHERE seq_1 IN (floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000)) AND seq_2 BETWEEN floor(rand()*1000000/2) AND floor(rand()*1000000)")
                            await conn.commit()
                        if (tbl_work_num == 4): ## SELECT
                            await cur.execute(f"SELECT * FROM test{rnd_num} WHERE seq_1 IN (floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000), floor(rand()*1000000)) AND seq_2 BETWEEN floor(rand()*1000000/2) AND floor(rand()*1000000)")
                            await conn.commit()
            except:
                print("ERROR")
                exit()

        # create count record
        tasks = [execute_query(self, pool, table_num, tbl_work_num) for i in range(iteration_num)]

        # execute asyncio
        await asyncio.gather(*tasks)

        # close pool
        pool.close()
        await pool.wait_closed()

        return self.start_time

if __name__=='__main__':
    global before_tbl_count
    before_tbl_count=0

    ## db connection info
    db_config = {
        'host': config.DB_CONNECTION_INFO['ENDPOINT'],
        'port': config.DB_CONNECTION_INFO['PORT'],
        'user': config.DB_CONNECTION_INFO['USER'],
        'password': config.DB_CONNECTION_INFO['PASSWD'],
        'db': config.DB_CONNECTION_INFO['DBNAME']
    }

    line=["━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
"┏━┓┏━┓   ┏━━━┳━━━┳┓   ┏━━┓┏┓ ┏┳┓  ┏┓┏━┓┏━━┳━┓ ┏┳━━━┳━━━┳━━━┳━━━━┓",
"┃┃┗┛┃┃   ┃┏━┓┃┏━┓┃┃   ┃┏┓┃┃┃ ┃┃┃  ┃┃┃┏┛┗┫┣┫┃┗┓┃┃┏━┓┃┏━━┫┏━┓┃┏┓┏┓┃",
"┃┏┓┏┓┣┓ ┏┫┗━━┫┃ ┃┃┃   ┃┗┛┗┫┃ ┃┃┃  ┃┗┛┛  ┃┃┃┏┓┗┛┃┗━━┫┗━━┫┗━┛┣┛┃┃┗┛",
"┃┃┃┃┃┃┃ ┃┣━━┓┃┃ ┃┃┃ ┏┓┃┏━┓┃┃ ┃┃┃ ┏┫┏┓┃  ┃┃┃┃┗┓┃┣━━┓┃┏━━┫┏┓┏┛ ┃┃",
"┃┃┃┃┃┃┗━┛┃┗━┛┃┗━┛┃┗━┛┃┃┗━┛┃┗━┛┃┗━┛┃┃┃┗┓┏┫┣┫┃ ┃┃┃┗━┛┃┗━━┫┃┃┗┓ ┃┃",
"┗┛┗┛┗┻━┓┏┻━━━┻━━┓┣━━━┛┗━━━┻━━━┻━━━┻┛┗━┛┗━━┻┛ ┗━┻━━━┻━━━┻┛┗━┛ ┗┛",
"     ┏━┛┃       ┗┛",
"     ┗━━┛                                             ..𝐛𝐲 𝐒𝐢𝐥𝐯𝐞𝐫",
"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
"\n 𝟏. 𝑰𝑵𝑺𝑬𝑹𝑻\n 𝟐. 𝑫𝑬𝑳𝑬𝑻𝑬\n 𝟑. 𝑼𝑷𝑫𝑨𝑻𝑬\n 𝟒. 𝑺𝑬𝑳𝑬𝑪𝑻\n\n 𝟏𝟎𝟎. 𝑪𝑹𝑬𝑨𝑻𝑬\n 𝟐𝟎𝟎. 𝑫𝑹𝑶𝑷\n 𝟑𝟎𝟎. 𝑻𝑹𝑼𝑵𝑪𝑨𝑻𝑬\n",
"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    print('\n'.join(line))
    bulkinsert = BulkInsert()
    try:
        while True:
            input_data=int(input("- 𝖨𝖭𝖯𝖴𝖳 𝖭𝖴𝖬𝖡𝖤𝖱: "))

            if (input_data == 0):
                    print("nice")
            elif (input_data == 1): ## INSERT
                try:
                    table_num=int(input("- 𝖭𝖴𝖬𝖡𝖤𝖱 𝖮𝖥 𝖨𝖭𝖲𝖤𝖱𝖳 𝖳𝖠𝖡𝖫𝖤𝖲(𝖣𝖤𝖥𝖠𝖴𝖫𝖳: 𝟣): ") or 1)
                    if (before_tbl_count < table_num):
                        table_num=table_num-before_tbl_count
                except ValueError:
                    continue

                start_time=asyncio.run(bulkinsert.async_io(table_num, input_data))

                end_time=datetime.now()
                print('𝐄𝐍𝐃 𝐓𝐈𝐌𝐄: ', end_time.isoformat(timespec='milliseconds'))

                elapsed_time=end_time-start_time
                print(f'𝐄𝐋𝐀𝐏𝐒𝐄𝐃 𝐓𝐈𝐌𝐄: {elapsed_time}\n')
            elif (input_data == 2): ## DELETE
                try:
                    table_num=int(input("- 𝖭𝖴𝖬𝖡𝖤𝖱 𝖮𝖥 𝖣𝖤𝖫𝖤𝖳𝖤 𝖳𝖠𝖡𝖫𝖤𝖲(𝖣𝖤𝖥𝖠𝖴𝖫𝖳: 𝟣): ") or 1)
                    if (before_tbl_count < table_num):
                        table_num=table_num-before_tbl_count
                except ValueError:
                    continue
                asyncio.run(bulkinsert.async_io(table_num, input_data))
                print(f'END TIME: {ctime()}')
            elif (input_data == 3): ## UPDATE
                try:
                    table_num=int(input("- 𝖭𝖴𝖬𝖡𝖤𝖱 𝖮𝖥 𝖴𝖯𝖣𝖠𝖳𝖤 𝖳𝖠𝖡𝖫𝖤𝖲(𝖣𝖤𝖥𝖠𝖴𝖫𝖳: 𝟣): ") or 1)
                    if (before_tbl_count < table_num):
                        table_num=table_num-before_tbl_count
                except ValueError:
                    continue
                asyncio.run(bulkinsert.async_io(table_num, input_data))
                print(f'END TIME: {ctime()}')
            elif (input_data == 4): ## SELECT
                try:
                    table_num=int(input("- 𝖭𝖴𝖬𝖡𝖤𝖱 𝖮𝖥 𝖲𝖤𝖫𝖤𝖢𝖳 𝖳𝖠𝖡𝖫𝖤𝖲(𝖣𝖤𝖥𝖠𝖴𝖫𝖳: 𝟣): ") or 1)
                    if (before_tbl_count < table_num):
                        table_num=table_num-before_tbl_count
                except ValueError:
                    continue
                asyncio.run(bulkinsert.async_io(table_num, input_data))
                print(f'END TIME: {ctime()}')

            elif (input_data == 100) or (input_data == 200) or (input_data == 300):
                work_iter_num=int(input("- 𝖭𝖴𝖬𝖡𝖤𝖱 𝖮𝖥 𝖳𝖠𝖡𝖫𝖤𝖲(𝖣𝖤𝖥𝖠𝖴𝖫𝖳: 𝟣): ") or 1)
                bulkinsert.cr_dr_tr_table(input_data, work_iter_num)
            else:
                print("𝙿𝚕𝚎𝚊𝚜𝚎, 𝚒𝚗𝚙𝚞𝚝 𝚌𝚘𝚕𝚕𝚎𝚌𝚝 𝚗𝚞𝚖𝚋𝚎𝚛.")
                continue

    except ValueError: # not numerical
        print("𝐄𝐗𝐈𝐓..")
    except KeyboardInterrupt: # ctrl+c
        print("\n\n𝐜𝐚𝐧𝐜𝐞𝐥𝐢𝐧𝐠...\n")
