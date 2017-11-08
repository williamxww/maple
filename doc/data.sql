
create table states (
  id int,
  name varchar(30)
);

create table test (
  id int,
  name varchar(30)
);


select t.id,t.name,s.id from test t inner join states s on t.id=s.id where t.id>0 order by t.id desc;

INSERT INTO states VALUES (1, 'Alabama');
INSERT INTO states VALUES (2, 'Alaska');
INSERT INTO states VALUES (3, 'Arizona');
INSERT INTO states VALUES (4, 'Arkansas');
INSERT INTO states VALUES (5, 'California');
INSERT INTO states VALUES (6, 'wuhan');
INSERT INTO states VALUES (7, '1');
INSERT INTO states VALUES (8, 'vv');
