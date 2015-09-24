# tornado_neo4j

Cypher query
```
users = yield self.db.query('MATCH (user:User) RETURN user')
```


Transaction
```
with self.db as transaction:
    peter = self.db.query('MATCH (user:User {name: {name}}) RETURN user', name='Peter')
    sam = self.db.query('MATCH (user:User {name: {name}}) RETURN user', name='Sam')
yield transaction
```