当要用某个在maple中的类时，直接拖拽到lab中修改

create table person (
  id int,
  age int
);

INSERT INTO person VALUES (1, 28);
INSERT INTO person VALUES (2, 27);
INSERT INTO person VALUES (3, 29);
INSERT INTO person VALUES (4, 26);

heapPage增加tuple
com.bow.lab.storage.SimpleTableServiceTest.createTable
com.bow.lab.storage.SimpleTableServiceTest.addTuple


btree增加node
CREATE INDEX idx_age ON person (age);
drop index idx_v;
ALTER TABLE person DROP INDEX idx_v;

能够创建索引文件
添加index tuple
通过索引快速定位到data tuple


模型和行为分离，pageTuple里不包含DBPage DBFile，在其子类中实现这部分功能