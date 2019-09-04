-- 查询主外键关系
SELECT tc.constraint_name,
       tc.table_name   as from_table,
       kcu.column_name as from_column,
       ccu.table_name  AS to_table,
       ccu.column_name AS to_column
FROM information_schema.table_constraints AS tc
         JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
         JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY' and ccu.table_name ~ '.*(?<!_ext)$'; -- 不是以 ‘_ext' 结尾的约束

-- 创建关系表
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

-- 还需要一个唯一键 from_table + from_column + to_table + to_column
-- 需要一个触发器

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
WHERE constraint_type = 'FOREIGN KEY' and ccu.table_name ~ '.*(?<!_ext)$';

-- 查询
select *
from mstdata.md_relation_metadata mrm;