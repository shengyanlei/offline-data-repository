use test;
-- 测试over函数的使用
-- 1.1 默认的开窗范围
truncate table test.shyl_over_test;
insert into table test.shyl_over_test values
("1","张三",10),
("1","张三",10),
("1","张三",20),
("1","张三",30),
("1","张三",40),
("1","张三",null);

select id,sum(grade) over(partition by id),row_number() over(partition by id) from test.shyl_over_test;
| id | sum\_window\_0 | row\_number\_window\_1 |
| :--- | :--- | :--- |
| 1 | 110 | 1 |
| 1 | 110 | 2 |
| 1 | 110 | 3 |
| 1 | 110 | 4 |
| 1 | 110 | 5 |
| 1 | 110 | 6 |
| 2 | 100 | 1 |
| 2 | 100 | 2 |
| 2 | 100 | 3 |
| 2 | 100 | 4 |
| 2 | 100 | 5 |


select id,sum(grade) over(order by id),row_number() over(order by id) from test.shyl_over_test;
| id | sum\_window\_0 | row\_number\_window\_1 |
| :--- | :--- | :--- |
| 1 | 110 | 1 |
| 1 | 110 | 2 |
| 1 | 110 | 3 |
| 1 | 110 | 4 |
| 1 | 110 | 5 |
| 1 | 110 | 6 |
| 2 | 210 | 7 |
| 2 | 210 | 8 |
| 2 | 210 | 9 |
| 2 | 210 | 10 |
| 2 | 210 | 11 |


select id,sum(grade) over(partition by id order by grade),row_number() over(partition by id order by grade) from test.shyl_over_test;
| id | sum\_window\_0 | row\_number\_window\_1 |
| :--- | :--- | :--- |
| 1 | null | 1 |
| 1 | 20 | 2 |
| 1 | 20 | 3 |
| 1 | 40 | 4 |
| 1 | 70 | 5 |
| 1 | 110 | 6 |
| 2 | null | 1 |
| 2 | 10 | 2 |
| 2 | 30 | 3 |
| 2 | 60 | 4 |
| 2 | 100 | 5 |
-- rows between 开始位置 and 结束位置
select id,sum(grade) over(partition by id order by grade rows between UNBOUNDED PRECEDING AND CURRENT ROW)from test.shyl_over_test;
| id | sum\_window\_0 |
| :--- | :--- |
| 1 | null |
| 1 | 10 |
| 1 | 20 |
| 1 | 40 |
| 1 | 70 |
| 1 | 110 |
| 2 | null |
| 2 | 10 |
| 2 | 30 |
| 2 | 60 |
| 2 | 100 |

select id,sum(grade) over(partition by id rows between UNBOUNDED PRECEDING AND CURRENT ROW)from test.shyl_over_test;
| id | sum\_window\_0 |
| :--- | :--- |
| 1 | null |
| 1 | 40 |
| 1 | 70 |
| 1 | 90 |
| 1 | 100 |
| 1 | 110 |
| 2 | 10 |
| 2 | 30 |
| 2 | 60 |
| 2 | 100 |
| 2 | 100 |

select id,sum(grade) over(partition by id order by grade desc rows between UNBOUNDED PRECEDING AND CURRENT ROW)from test.shyl_over_test;
| id | sum\_window\_0 |
| :--- | :--- |
| 1 | 40 |
| 1 | 70 |
| 1 | 90 |
| 1 | 100 |
| 1 | 110 |
| 1 | 110 |

select id,sum(grade) over(partition by id order by grade desc rows between CURRENT ROW AND CURRENT ROW)from test.shyl_over_test;
| id | sum\_window\_0 |
| :--- | :--- |
| 1 | 40 |
| 1 | 30 |
| 1 | 20 |
| 1 | 10 |
| 1 | 10 |
| 1 | 0 |
select id,grade,sum(grade) over(rows between CURRENT ROW AND CURRENT ROW) from test.shyl_over_test;
select id,grade,sum(grade) over( order by grade rows between 2 preceding and 2 following ) from test.shyl_over_test;
select id,grade,sum(grade) over(rows between unbounded  preceding and unbounded following ) from test.shyl_over_test;
| id | grade | sum\_window\_0 |
| 1 | null | 110 |
| 1 | 40 | 110 |
| 1 | 30 | 110 |
| 1 | 20 | 110 |
| 1 | 10 | 110 |
| 1 | 10 | 110 |

