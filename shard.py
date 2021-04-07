import psycopg2

from hashlib import md5
from uhashring import HashRing


class ShardDb:
    @staticmethod
    def get_engine(port: int):
        return psycopg2.connect(
            "dbname='postgres' user='postgres' host='localhost' password='password'",
            port=port
        )

    def __init__(self):
        self._shard1_engine = self.get_engine(5432)
        self._shard2_engine = self.get_engine(5433)
        self._shard3_engine = self.get_engine(5434)
        self._shard1_cur: psycopg2.extensions.cursor = self._shard1_engine.cursor()
        self._shard2_cur: psycopg2.extensions.cursor = self._shard2_engine.cursor()
        self._shard3_cur: psycopg2.extensions.cursor = self._shard3_engine.cursor()
        shard1, shard2, shard3 = "shard1", "shard2", "shard3"
        self.ring = HashRing(nodes=[shard1, shard2, shard3])
        self.shard_connections = {
            shard1: self._shard1_engine,
            shard2: self._shard2_engine,
            shard3: self._shard3_engine,
        }

        self.shard_cursors = {
            shard1: self._shard1_cur,
            shard2: self._shard2_cur,
            shard3: self._shard3_cur,
        }

    def select_node(self, url: str):
        return self.ring.get_node(url)

    @staticmethod
    def digest_url(url: str):
        return md5(url.encode()).hexdigest()[:5]

    def insert_url(self, url: str):
        url_id = self.digest_url(url=url)
        node = self.select_node(url=url_id)
        print(f"inserting '{url}' to: {node}")
        selected_shard = self.shard_cursors[node]
        selected_shard.execute("""
                insert into url_table(url, url_id) values (%s, %s)
            """, (url, url_id))
        selected_con = self.shard_connections[node]
        selected_con.commit()

    def get_url(self, url: str):
        url_id = self.digest_url(url=url)
        node = self.select_node(url=url_id)
        selected_node = self.shard_cursors[node]
        selected_node.execute(
            "select * from url_table where url_id = %s", ((url_id,))
        )
        return selected_node.fetchall()


if __name__ == "__main__":
    shard_db = ShardDb()
    urls = [
            "www.wikipedia/sharding",
            "www.netflix.com",
            "www.stackoverflow.com",
            "www.seekingalpha.com",
            "www.wikipedia/postgres",
            "www.google.com",
            "www.instagram.com",
        ]

    for u in urls:
        # shard_db.insert_url(url=u)
        print(f"querying url {u}")
        for res in shard_db.get_url(url=u):
            print(res)
        print()
    