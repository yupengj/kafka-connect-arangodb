# kafka-connect-arangodb

arangodb 连接器可以实现根据 kafka 中的数据创建点，也可以根据 kafka 中指定边元数据主题创建边。

## 可以做什么
- 可以根据 kafka 中主题中的数据创建顶点
- 可以根据 kafka 指定主题中的边的元数据（关系数据库中的外键约束）创建边的关系。

## 配置
| Name                        | Description                         | Type     | Default | Importance |
| --------------------------- | ----------------------------------- | -------- | ------- | ---------- |
| `arangodb.host`             | ArangoDB server host.               | string   |         | high       |
| `arangodb.port`             | ArangoDB server host port number.   | int      |         | high       |
| `arangodb.user`             | ArangoDB connection username.       | string   |         | high       |
| `arangodb.password`         | ArangoDB connection password.       | password | ""      | high       |
| `arangodb.database.name`    | ArangoDB database name.             | string   |         | high       |
/ `edge.metadata.kafka.servers` |边元数据主题所在的 kafka 地址         | string  |          | high       |
| `arangodb.user`             | ArangoDB connection username.       | string   |         | high       |
| `arangodb.password`         | ArangoDB connection password.       | password | ""      | high       |
| `arangodb.database.name`    | ArangoDB database name.             | string   |         | high       |

## 使用


1. **在关系数据库中增加边元数据表，由外键约束构成。**
> 注意：创建启动 arangodb 连接器时一定要确保这个表已经存在，如果不存在则不会创建边。



