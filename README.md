# kafka-connect-arangodb

arangodb 连接器可以实现根据 kafka 中的数据创建点，也可以根据 kafka 中指定边元数据主题创建边。

## 可以做什么
- 可以根据 kafka 中主题中的数据创建顶点
- 可以根据 kafka 指定主题中的边的元数据（关系数据库中的外键约束）创建边的关系。

### 关于顶点(每条数据)
顶点就是每条数据，关系数据库中的每行记录

### 关于边和边的元数据
- 边的元数据是在关系数据库中根据外键约束生成脚本如下：
```sql
drop table mstdata.md_relation_metadata;
CREATE TABLE if not exists mstdata.md_relation_metadata
(
    id              serial primary key,
    constraint_name text,
    from_table      text,
    from_column     text,
    to_table        text,
    to_column       text
);
-- 插入数据
insert into mstdata.md_relation_metadata(constraint_name, from_table, from_column, to_table, to_column)
SELECT tc.constraint_name,
       tc.table_name   as from_table,
       kcu.column_name as from_column,
       ccu.table_name  AS to_table,
       ccu.column_name AS to_column
FROM information_schema.table_constraints AS tc
         JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
         JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY';
```

- 只有边的元数据存在时才会创建边的数据，边的元数据中每条记录定义了一种关系，每种关系都会在 arangodb 数据库中生成一个 collection，集合的名称是 "constraint_name" 字段值
- 如果顶点的创建在边元数据之前，那么和顶点关联的边不会自动创建。这时可以在边的元数据创建后，批量更新顶点所对应的表，触发向 arangodb 数据库写数据。来完成创建边
- 删除边的元数据时会删除边元数据对应的 collection
- 连接器中不处理边的元数据修改操作

## 配置参数
| 名称                        | 描述                                 | 类型     | 默认值   | 重要等级    |
| --------------------------- | ----------------------------------- | -------- | ------- | ---------- |
| `arangodb.host`             | ArangoDB server host.               | string   |         | high       |
| `arangodb.port`             | ArangoDB server host port number.   | int      |         | high       |
| `arangodb.user`             | ArangoDB connection username.       | string   |         | high       |
| `arangodb.password`         | ArangoDB connection password.       | password | ""      | high       |
| `arangodb.database.name`    | ArangoDB database name.             | string   |         | high       |
| `edge.metadata.kafka.servers` | 边元数据主题所在的 kafka 地址         | string  |          | high      |
| `edge.metadata.topic`         | 边元数据主题名称.                     | string |         | high       |
| `edge.metadata.attribute.map` | 连接器中边元数据属性和 kafka 中边元数据主题字段映射 | string | 如下| high |

> edge.metadata.attribute.map 默认值： "key:id,edgeCollection:constraint_name,fromCollection:from_table,fromAttribute:from_column,toCollection:to_table,
toAttribute:to_column"

## 使用

1. doc/jar 文件夹下有两个 jar 包 `arangodb-java-driver-6.0.0-SNAPSHOT-all.jar` 是 arangodb 数据库的java驱动包基本不会变。`kafka-connect-arangodb-2.0-SNAPSHOT.jar` 是连接器源码包会随着修改bug儿变化
2. doc/arangodb.json 文件是连接器的配置。

## 更新记录

- [x] 自动创建、修改、删除顶点
- [x] 根据边的元数据和顶点自动创建边
- [x] 根据顶点的修改、删除自动修改、删除边
- [x] 处理边元数据的新增和删除
- [x] 删除边 aql 语句优化
- [x] 扩展表不同步到 arangodb 中
- [x] bom主表不同步
- [x] 测试批量导入和删除 BOM 数据连接器的性能 (1w条数据同步到arangodb大约30秒)

## 接下来计划
- [ ] 元数据边采用触发器维护（不一定可行，如不可行采用手动维护）


----
## 相关文件

- postgres 连接器配置文件 [postgres.json](doc/postgres.json)
- arangodb 连接器配置文件 [arangodb.json](doc/arangodb.json)
- arangodb 连接器 arangodb-java 驱动包 [arangodb-java-driver-6.0.0-SNAPSHOT-all.jar](doc/jar/arangodb-java-driver-6.0.0-SNAPSHOT-all.jar)
- arangodb 连接器源码包 [kafka-connect-arangodb-2.0-SNAPSHOT.jar](doc/jar/kafka-connect-arangodb-2.0-SNAPSHOT.jar)
- postgres 数据库中创建边元数据脚本文件 [md_relation_metadata.sql](doc/md_relation_metadata.sql)