from tornado.testing import AsyncTestCase, gen_test
from database import Request, Database


class DatabaseTestCase(AsyncTestCase):

    def setUp(self):
        super(DatabaseTestCase, self).setUp()
        def _response(db):
            self.db = db()
            self.stop()
        Database.init(self.io_loop, _response)
        self.wait()

    @gen_test
    def test_cypher_query(self):
        data = yield self.db.query('MATCH (n:NO_LABLE) RETURN n')
        self.assertEqual(data, [])

    @gen_test
    def test_cypher_query_with_transaction(self):
        with self.db as transaction:
            test1 = self.db.query('MATCH (n:NO_LABLE) RETURN n')
            test2 = self.db.query('MATCH (n:NO_LABLE) RETURN n')
        yield transaction
        self.assertEqual(test1, [])
        self.assertEqual(test2, [])