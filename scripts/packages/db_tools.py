import psycopg2.extras
import psycopg2

# conn = None
# try:
#     # Password is contained in a file so that it isn't exposed here.
#     conn = psycopg2.connect("db = 'ins_data_dev' user = 'andydixon' host = 'localhost'")
# except:
#     print("I am unable to connect the database")


class MyDatabase:
    def __init__(self, db="ins_data_dev", user="andydixon", host="localhost"):
        self.conn = psycopg2.connect(database=db, user=user, host=host)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        self.proc_cur = self.conn.cursor()

    def query(self, query, args, return_result):
        print(self.cur.mogrify(query, args))
        self.cur.execute(query, args)
        if return_result is True:
            rs = self.cur.fetchall()
            print(rs)
            return rs
        self.conn.commit()

    def call_proc(self, pg_function, args):
        self.proc_cur.callproc(pg_function, args)
        self.conn.commit()

    def copy_expert(self, query, args):
        self.proc_cur.copy_expert(query, args)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
