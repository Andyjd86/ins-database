from psycopg2.extras import *
import psycopg2

# TODO 1 - Read DB credentials from an INI file, this will allow for multiple database connections in the same script.
# TODO 2 - Understand Classes in more detail.


class MyDatabase:
    # def __init__(self, db="ins_data_dev", user="andydixon", host="localhost", port="5666"):
    # def __init__(self, db="24850_hs2", user="adixon", host="10.111.10.42", port="5446", password="@Uckland86"):
    def __init__(self, db="24850_hs2", user="andydixon", host="localhost", port="5666"):
        self.conn = psycopg2.connect(database=db, user=user, host=host, port=port)
        # self.conn = psycopg2.connect(database=db, user=user, host=host, port=port, password=password)
        self.cur = self.conn.cursor(cursor_factory=NamedTupleCursor)
        self.proc_cur = self.conn.cursor()

    def standard_query(self, query, args, return_result):
        print(self.cur.mogrify(query, args))
        self.cur.execute(query, args)
        if return_result is True:
            rs = self.cur.fetchall()
            print(rs)
            self.conn.commit()
            return rs
        self.conn.commit()

    def maintenance_query(self, query, args, return_result):
        print(self.cur.mogrify(query, args))
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)
        self.cur.execute(query, args)
        if return_result is True:
            rs = self.cur.fetchall()
            print(rs)
            self.conn.commit()
            self.conn.set_isolation_level(old_isolation_level)
            return rs
        self.conn.commit()
        self.conn.set_isolation_level(old_isolation_level)

    def execute_lots(self, query, args, template, return_result):
        # print(self.cur.mogrify(query, args))
        execute_values(self.proc_cur, query, args, template, page_size=1000)
        if return_result is True:
            rs = self.cur.fetchall()
            print(rs)
            self.conn.commit()
            return rs
        self.conn.commit()

    def call_proc(self, pg_function, args):
        self.proc_cur.callproc(pg_function, args)
        self.conn.commit()

    def mogrification(self, query, args):
        print(self.cur.mogrify(query, args))

    def copy_expert(self, query, args):
        self.proc_cur.copy_expert(query, args)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
