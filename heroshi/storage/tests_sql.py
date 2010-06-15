# coding: utf-8
import unittest

from . import sql


class Is_safe(unittest.TestCase):
    def test_001(self):
        self.assertFalse(sql.is_safe("foo"))

    def test_002(self):
        self.assertTrue(sql.is_safe(sql.mark_safe("foo")))


class Escape(unittest.TestCase):
    def test_001(self):
        escaped_text = sql.escape("abc")
        expected_text = '"abc"'
        self.assertEqual(escaped_text, expected_text)

    def test_002(self):
        escaped_text = sql.escape("ab;c")
        expected_text = '"ab;c"'
        self.assertEqual(escaped_text, expected_text)

    def test_003(self):
        escaped_text = sql.escape("ab\"c")
        expected_text = '"ab""c"'
        self.assertEqual(escaped_text, expected_text)

    def test_004(self):
        escaped_text = sql.escape("abc.def")
        expected_text = '"abc"."def"'
        self.assertEqual(escaped_text, expected_text)

    def test_005(self):
        escaped_text = sql.escape("abc.   def")
        expected_text = '"abc"."def"'
        self.assertEqual(escaped_text, expected_text)

    def test_006(self):
        escaped_text = sql.escape("abc   .   def")
        expected_text = '"abc"."def"'
        self.assertEqual(escaped_text, expected_text)

    def test_007(self):
        escaped = sql.escape("10")
        expected = '10'
        self.assertEqual(escaped, expected)

    def test_008(self):
        self.assertRaises(ValueError, sql.escape, "\0")


class Where(unittest.TestCase):
    def test_001(self):
        query, params = sql.where(a=10)
        self.assertEqual(query, 'WHERE ("a" = %s)')
        self.assertEqual(params, [10])

    def test_002(self):
        query, params = sql.where(a=10, b=20)
        self.assertEqual(query, 'WHERE ("a" = %s) AND ("b" = %s)')
        self.assertEqual(params, [10, 20])

    def test_003(self):
        query, params = sql.where(foo=None)
        self.assertEqual(query, 'WHERE ("foo" IS NULL)')
        self.assertEqual(params, [])

    def test_in_001(self):
        query, params = sql.where(a=[1, 2, 3])
        self.assertEqual(query, 'WHERE ("a" IN (%s, %s, %s))')
        self.assertEqual(params, [1,2,3])

    def test_in_002(self):
        query, params = sql.where(a=[1, 2, None])
        self.assertEqual(query, 'WHERE (("a" IS NULL) OR ("a" IN (%s, %s)))')
        self.assertEqual(params, [1,2])

    def test_in_003(self):
        query, params = sql.where(a=[1, None, 2])
        self.assertEqual(query, 'WHERE (("a" IS NULL) OR ("a" IN (%s, %s)))')
        self.assertEqual(params, [1,2])

    def test_in_004(self):
        query, params = sql.where(a=[None, 1, 2])
        self.assertEqual(query, 'WHERE (("a" IS NULL) OR ("a" IN (%s, %s)))')
        self.assertEqual(params, [1,2])

    def test_in_005(self):
        """Must correctly generate IN operator when column ends with 'in'"""
        query, params = sql.where(domain__in=['a', 'b', 'c'])
        self.assertEqual(query, 'WHERE ("domain" IN (%s, %s, %s))')
        self.assertEqual(params, ['a','b','c'])

    def test_in_006(self):
        """Must generate IS NULL for None values"""
        query, params = sql.where(foo=[None])
        self.assertEqual(query, 'WHERE ("foo" IS NULL)')
        self.assertEqual(params, [])

    def test_in_007(self):
        """Must generate IS NULL for None values in list"""
        query, params = sql.where(foo=[1, None])
        self.assertEqual(query, 'WHERE (("foo" IS NULL) OR ("foo" IN (%s)))')
        self.assertEqual(params, [1])

    def test_in_008(self):
        """Must generate only single IS NULL for any number of None values in list"""
        query, params = sql.where(foo=[1, None, None, None])
        self.assertEqual(query, 'WHERE (("foo" IS NULL) OR ("foo" IN (%s)))')
        self.assertEqual(params, [1])

    def test_starts_001(self):
        query, params = sql.where(foo__starts="He")
        self.assertEqual(query, 'WHERE ("foo" LIKE %s)')
        self.assertEqual(params, ["He%"])

    def test_ends_001(self):
        query, params = sql.where(foo__ends="d!")
        self.assertEqual(query, 'WHERE ("foo" LIKE %s)')
        self.assertEqual(params, ["%d!"])

    def test_like_001(self):
        query, params = sql.where(foo__like="bar")
        self.assertEqual(query, 'WHERE ("foo" LIKE %s)')
        self.assertEqual(params, ["%bar%"])


class Delete(unittest.TestCase):
    def test_001(self):
        query, params = sql.delete("foo", a__in=[1,2,3])
        self.assertEqual(query, 'DELETE FROM "foo" WHERE ("a" IN (%s, %s, %s));')
        self.assertEqual(params, [1,2,3])

    def test_002(self):
        query, params = sql.delete("foo", a__in=[1,2,3])
        self.assertEqual(query, 'DELETE FROM "foo" WHERE ("a" IN (%s, %s, %s));')
        self.assertEqual(params, [1,2,3])


class Select(unittest.TestCase):
    def test_001(self):
        query, params = sql.select("foo")
        self.assertEqual(query, 'SELECT * FROM "foo";')
        self.assertEqual(params, [])

    def test_002(self):
        query, params = sql.select("foo", limit=10, offset=15)
        self.assertEqual(query, 'SELECT * FROM "foo" LIMIT 10 OFFSET 15;')
        self.assertEqual(params, [])

    def test_003(self):
        query, params = sql.select("foo", a=10, b=20)
        self.assertEqual(query, 'SELECT * FROM "foo" WHERE ("a" = %s) AND ("b" = %s);')
        self.assertEqual(params, [10, 20])

    def test_004(self):
        query, params = sql.select("foo", columns=['a'], a=10, b=20)
        self.assertEqual(query, 'SELECT "a" FROM "foo" WHERE ("a" = %s) AND ("b" = %s);')
        self.assertEqual(params, [10, 20])

    def test_005(self):
        query, params = sql.select("foo", columns=['a'], order_by=['a'])
        self.assertEqual(query, 'SELECT "a" FROM "foo" ORDER BY "a" ASC;')
        self.assertEqual(params, [])

    def test_006(self):
        query, params = sql.select("foo", columns=['a'], order_by=['a', 'b'])
        self.assertEqual(query, 'SELECT "a" FROM "foo" ORDER BY "a" ASC, "b" ASC;')
        self.assertEqual(params, [])

    def test_007(self):
        query, params = sql.select("foo", columns=['a'], order_by=['-a'])
        self.assertEqual(query, 'SELECT "a" FROM "foo" ORDER BY "a" DESC;')
        self.assertEqual(params, [])

    def test_008(self):
        query, params = sql.select("foo", columns=['a'], order_by=['-a', 'b'])
        self.assertEqual(query, 'SELECT "a" FROM "foo" ORDER BY "a" DESC, "b" ASC;')
        self.assertEqual(params, [])

    def test_009(self):
        query, params = sql.select(sql.subquery('foo', as_='f'), columns=['a'], order_by=['-a', 'b'])
        self.assertEqual(query, 'SELECT "a" FROM (SELECT * FROM "foo") AS "f" ORDER BY "a" DESC, "b" ASC;')
        self.assertEqual(params, [])

    def test_010(self):
        query, params = sql.select("foo", columns=['a', sql.sum_('b')], group_by=['a'])
        self.assertEqual(query, 'SELECT "a", SUM("b") FROM "foo" GROUP BY "a";')

    def test_groupby_001(self):
        query, params = sql.select("foo", columns=['a'], group_by=['a'])
        self.assertEqual(query, 'SELECT "a" FROM "foo" GROUP BY "a";')

    def test_groupby_002(self):
        query, params = sql.select("foo", columns=['a', 'b'], group_by=['a', 'b'])
        self.assertEqual(query, 'SELECT "a", "b" FROM "foo" GROUP BY "a", "b";')


class Insert(unittest.TestCase):
    def test_001(self):
        query, params = sql.insert("foo", {"a": 10, "b": 20})
        self.assertEqual(query, 'INSERT INTO "foo" ("a", "b") VALUES (%s, %s);')
        self.assertEqual(params, [10, 20])


class Update(unittest.TestCase):
    def test_001(self):
        query, params = sql.update("foo", {"a": 10, "b":20 })
        self.assertEqual(query, 'UPDATE "foo" SET "a"=%s, "b"=%s;')
        self.assertEqual(params, [10, 20])

    def test_002(self):
        query, params = sql.update("foo", {"a": 10, "b":20 }, {"a": 10})
        self.assertEqual(query, 'UPDATE "foo" SET "a"=%s, "b"=%s WHERE ("a" = %s);')
        self.assertEqual(params, [10, 20, 10])


class As(unittest.TestCase):
    def test_001(self):
        query = sql.as_("foo", "f")
        self.assertEqual(query, '"foo" AS "f"')

    def test_002(self):
        query = sql.as_("foo-bar", "f")
        self.assertEqual(query, '"foo-bar" AS "f"')

    def test_003(self):
        query = sql.as_("foo.bar", "f")
        self.assertEqual(query, '"foo"."bar" AS "f"')

    def test_004(self):
        query = sql.as_("foo", "\"f")
        self.assertEqual(query, '"foo" AS """f"')


class Sum(unittest.TestCase):
    def test_001(self):
        query = sql.sum_("foo")
        self.assertEqual(query, 'SUM("foo")')

    def test_002(self):
        query = sql.sum_("bar", as_="b")
        self.assertEqual(query, 'SUM("bar") AS "b"')

    def test_003(self):
        query = sql.sum_("foo.bar__count")
        self.assertEqual(query, 'SUM("foo"."bar__count")')


class Extract(unittest.TestCase):
    def test_001(self):
        """Must return safe SQL"""
        query = sql.extract("foo", "bar")
        self.assertTrue(sql.is_safe(query))

    def test_002(self):
        query = sql.extract("foo", "bar")
        self.assertEqual(query, 'EXTRACT(foo FROM "bar")')

    def test_003(self):
        bar = sql.escape("bar")
        query = sql.extract("foo", bar)
        self.assertEqual(query, 'EXTRACT(foo FROM %s)' % (bar,))


class Date_trunc(unittest.TestCase):
    def test_001(self):
        """Must return safe SQL"""
        query = sql.date_trunc("day", "created")
        self.assertTrue(sql.is_safe(query))

    def test_002(self):
        query = sql.date_trunc("day", "created")
        self.assertEqual(query, '''DATE_TRUNC('day', "created")''')

    def test_003(self):
        bar = sql.escape("bar")
        query = sql.date_trunc("day", bar)
        self.assertEqual(query, '''DATE_TRUNC('day', %s)''' % (bar,))


class Count(unittest.TestCase):
    def test_001(self):
        """Must return safe SQL"""
        query = sql.count("foo")
        self.assertTrue(sql.is_safe(query))

    def test_002(self):
        query = sql.count("foo")
        self.assertEqual(query, 'COUNT("foo")')


class Lower(unittest.TestCase):
    def test_001(self):
        """Must return safe SQL"""
        query = sql.lower("foo")
        self.assertTrue(sql.is_safe(query))

    def test_002(self):
        query = sql.lower("foo")
        self.assertEqual(query, 'LOWER("foo")')


class Upper(unittest.TestCase):
    def test_001(self):
        """Must return safe SQL"""
        query = sql.upper("foo")
        self.assertTrue(sql.is_safe(query))

    def test_002(self):
        query = sql.upper("foo")
        self.assertEqual(query, 'UPPER("foo")')


class Coalesce(unittest.TestCase):
    def test_001(self):
        """Must return safe SQL"""
        query = sql.coalesce("foo")
        self.assertTrue(sql.is_safe(query))

    def test_002(self):
        query = sql.coalesce("foo")
        self.assertEqual(query, 'COALESCE("foo")')

    def test_003(self):
        query = sql.coalesce("foo", "bar", "zar")
        self.assertEqual(query, 'COALESCE("foo", "bar", "zar")')
