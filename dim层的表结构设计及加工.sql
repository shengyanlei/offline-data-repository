-- date:2023-09-15
-- author:@shyl
-- 问题一：为什么使用snappy压缩后，desc formatted【compress no】 和 show create table 【snappy】 展示是否压缩 不一致：
-- 还没弄懂为什么 【可能hive 没有配置snappy算法，但是文件大小确实不一致】
-- 写在前面：
-- 表使用orc存储格式，不使用压缩算法
-- datagrip 大小写转换  ctrl+shift+u
create database shyl_dim;
-- hive配置
-- 表示启用动态分区功能。
set hive.exec.dynamic.partition=true;
-- 使用非严格模式，允许动态分区。
set hive.exec.dynamic.partition.mode=nonstrict;
-- 以用户维度表为例对比一下各存储格式，压缩算法   样例数据第一天200条
--------------------------------------------
-- | 存储格式    | 压缩算法  |天数        |数据量    |占用空间
-- |textfile    |         |2023-08-18 |  200条  |26.4kb
-- |textfile    |gzip     |2023-08-18 |  200条  |26.4kb  无压缩
-- |orc         |         |2023-08-18 |  200条  |8.8kb
-- |orc         |snappy   |2023-08-18 |  200条  |12.69Kb
-- |parquet     |         |2023-08-18 |  200条  |20.37kb
-- 用户维度表（拉链表）
-- ========================================================textfile
drop table if exists shyl_dim.dim_user_info_textfile;
create table if not exists shyl_dim.dim_user_info_textfile
(
    user_id       string comment '用户id' ,
    login_name    string comment '用户名称' ,
    nick_name     string comment '用户昵称' ,
    user_name     string comment '姓名' ,
    gender        string comment '性别' ,
    birthday      string comment '生日' ,
    phone         string comment '电话号' ,
    email         string comment '电子邮箱' ,
    user_level    string comment '用户等级' ,
    status        string comment '状态' ,
    create_time   string comment '创建时间' ,
    operator_time string comment '修改日期' ,
    start_time    string comment '开始时间' ,
    end_time      string comment '结束时间'
) comment '用户信息拉链表'
    partitioned by (dt string comment "分区字段yyyy-MM-dd")
    stored as textfile;

-- 不压缩 textfile 26.4kb
insert overwrite table shyl_dim.dim_user_info_textfile partition (dt)
select
    `data`.id                                              as id
    , `data`.login_name                                    as login_name
    , `data`.nick_name                                     as nick_name
    , `data`.name                                          as user_name
    , if( `data`.gender is null , '未知' , `data`.gender ) as gender
    , `data`.birthday                                      as birthday
    , `data`.phone_num                                     as phone
    , `data`.email                                         as email
    , `data`.user_level                                    as user_level
    , `data`.status                                        as status
    , `data`.create_time                                   as create_time
    , `data`.operate_time                                  as operator_time
    , `data`.create_time                                   as start_time
    , '9999-12-31'                                         as end_time
    , dt
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
    and type = 'bootstrap-insert';
-- ==================================================================orc

drop table if exists shyl_dim.dim_user_info_orc;
create table if not exists shyl_dim.dim_user_info_orc
(
    user_id       string comment '用户id' ,
    login_name    string comment '用户名称' ,
    nick_name     string comment '用户昵称' ,
    user_name     string comment '姓名' ,
    gender        string comment '性别' ,
    birthday      string comment '生日' ,
    phone         string comment '电话号' ,
    email         string comment '电子邮箱' ,
    user_level    string comment '用户等级' ,
    status        string comment '状态' ,
    create_time   string comment '创建时间' ,
    operator_time string comment '修改日期' ,
    start_time    string comment '开始时间' ,
    end_time      string comment '结束时间'
) comment '用户信息拉链表'
    partitioned by (dt string comment "分区字段yyyy-MM-dd")
    stored as orc;
-- 未压缩 200条 8.8kb
insert overwrite table shyl_dim.dim_user_info_orc partition (dt)
select
    `data`.id                                              as id
    , `data`.login_name                                    as login_name
    , `data`.nick_name                                     as nick_name
    , `data`.name                                          as user_name
    , if( `data`.gender is null , '未知' , `data`.gender ) as gender
    , `data`.birthday                                      as birthday
    , `data`.phone_num                                     as phone
    , `data`.email                                         as email
    , `data`.user_level                                    as user_level
    , `data`.status                                        as status
    , `data`.create_time                                   as create_time
    , `data`.operate_time                                  as operator_time
    , `data`.create_time                                   as start_time
    , '9999-12-31'                                         as end_time
    , dt
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
    and type = 'bootstrap-insert';

-- ==================================================================parquet

drop table if exists shyl_dim.dim_user_info_parquet;
create table if not exists shyl_dim.dim_user_info_parquet
(
    user_id       string comment '用户id' ,
    login_name    string comment '用户名称' ,
    nick_name     string comment '用户昵称' ,
    user_name     string comment '姓名' ,
    gender        string comment '性别' ,
    birthday      string comment '生日' ,
    phone         string comment '电话号' ,
    email         string comment '电子邮箱' ,
    user_level    string comment '用户等级' ,
    status        string comment '状态' ,
    create_time   string comment '创建时间' ,
    operator_time string comment '修改日期' ,
    start_time    string comment '开始时间' ,
    end_time      string comment '结束时间'
) comment '用户信息拉链表'
    partitioned by (dt string comment "分区字段yyyy-MM-dd")
    stored as parquet;
-- 未压缩 200条 8.8kb
insert overwrite table shyl_dim.dim_user_info_parquet partition (dt)
select
    `data`.id                                              as id
    , `data`.login_name                                    as login_name
    , `data`.nick_name                                     as nick_name
    , `data`.name                                          as user_name
    , if( `data`.gender is null , '未知' , `data`.gender ) as gender
    , `data`.birthday                                      as birthday
    , `data`.phone_num                                     as phone
    , `data`.email                                         as email
    , `data`.user_level                                    as user_level
    , `data`.status                                        as status
    , `data`.create_time                                   as create_time
    , `data`.operate_time                                  as operator_time
    , `data`.create_time                                   as start_time
    , '9999-12-31'                                         as end_time
    , dt
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
    and type = 'bootstrap-insert';
-- =============================================================snappy
drop table if exists shyl_dim.dim_user_info_snappy;
create table if not exists shyl_dim.dim_user_info_snappy
(
    user_id       string comment '用户id' ,
    login_name    string comment '用户名称' ,
    nick_name     string comment '用户昵称' ,
    user_name     string comment '姓名' ,
    gender        string comment '性别' ,
    birthday      string comment '生日' ,
    phone         string comment '电话号' ,
    email         string comment '电子邮箱' ,
    user_level    string comment '用户等级' ,
    status        string comment '状态' ,
    create_time   string comment '创建时间' ,
    operator_time string comment '修改日期' ,
    start_time    string comment '开始时间' ,
    end_time      string comment '结束时间'
) comment '用户信息拉链表'
    partitioned by (dt string comment "分区字段yyyy-MM-dd")
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'snappy');

-- snappy 压缩 12.69kb
insert overwrite table shyl_dim.dim_user_info_snappy partition (dt)
select
    `data`.id                                              as id
    , `data`.login_name                                    as login_name
    , `data`.nick_name                                     as nick_name
    , `data`.name                                          as user_name
    , if( `data`.gender is null , '未知' , `data`.gender ) as gender
    , `data`.birthday                                      as birthday
    , `data`.phone_num                                     as phone
    , `data`.email                                         as email
    , `data`.user_level                                    as user_level
    , `data`.status                                        as status
    , `data`.create_time                                   as create_time
    , `data`.operate_time                                  as operator_time
    , `data`.create_time                                   as start_time
    , '9999-12-31'                                         as end_time
    , dt
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
    and type = 'bootstrap-insert';

-- ===========================================================gzip
drop table if exists shyl_dim.dim_user_info_gzip;
create table if not exists shyl_dim.dim_user_info_gzip
(
    user_id       string comment '用户id' ,
    login_name    string comment '用户名称' ,
    nick_name     string comment '用户昵称' ,
    user_name     string comment '姓名' ,
    gender        string comment '性别' ,
    birthday      string comment '生日' ,
    phone         string comment '电话号' ,
    email         string comment '电子邮箱' ,
    user_level    string comment '用户等级' ,
    status        string comment '状态' ,
    create_time   string comment '创建时间' ,
    operator_time string comment '修改日期' ,
    start_time    string comment '开始时间' ,
    end_time      string comment '结束时间'
) comment '用户信息拉链表'
    partitioned by (dt string comment "分区字段yyyy-MM-dd")
    stored as textfile
    TBLPROPERTIES ('serde.compress' = 'gzip');

-- gzip 压缩 26.4kb
insert overwrite table shyl_dim.dim_user_info_gzip partition (dt)
select
    `data`.id                                              as id
    , `data`.login_name                                    as login_name
    , `data`.nick_name                                     as nick_name
    , `data`.name                                          as user_name
    , if( `data`.gender is null , '未知' , `data`.gender ) as gender
    , `data`.birthday                                      as birthday
    , `data`.phone_num                                     as phone
    , `data`.email                                         as email
    , `data`.user_level                                    as user_level
    , `data`.status                                        as status
    , `data`.create_time                                   as create_time
    , `data`.operate_time                                  as operator_time
    , `data`.create_time                                   as start_time
    , '9999-12-31'                                         as end_time
    , dt
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
    and type = 'bootstrap-insert';

=============================================================lzo
-- 加工口径
show partitions shyl_ods.ods_user_info_inc;
select *
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
limit 10;
-- 数据样例
-- bootstrap-insert
-- | 1692344804 | {"id":"1","login\_name":"d0gkp633","nick\_name":"阿乐","passwd":null,"name":"令狐乐",
-- "phone\_num":"13148885648","email":"d0gkp633@126.com","head\_img":null,"user\_level":"1","birthday":"1994-02-18",
-- "gender":"M","create\_time":"2023-08-18 15:33:09","operate\_time":null,"status":null} | NULL | 2023-08-18 |

-- 开始加工dim层数据================================================================
-- 用户表 --拉链表
-- 数据探查
select *
from
    shyl_ods.ods_user_info_inc
where
    dt = '2023-08-19'
limit 10;
-- ：第一天直接解析字段即可
insert overwrite table shyl_dim.dim_user_info_orc partition (dt)
select
    `data`.id                                              as id
    , `data`.login_name                                    as login_name
    , `data`.nick_name                                     as nick_name
    , `data`.name                                          as user_name
    , if( `data`.gender is null , '未知' , `data`.gender ) as gender
    , `data`.birthday                                      as birthday
    , `data`.phone_num                                     as phone
    , `data`.email                                         as email
    , `data`.user_level                                    as user_level
    , `data`.status                                        as status
    , `data`.create_time                                   as create_time
    , `data`.operate_time                                  as operator_time
    , `data`.create_time                                   as start_time
    , '9999-12-31'                                         as end_time
    , dt
from
    shyl_ods.ods_user_info_inc
where
    dt = ${hivevar:etl_dt}
    and type = 'bootstrap-insert';
-- :由于用户表是拉链表，所以之后每天的数据处理方式：先将insert类型的数据和昨天的union在一起 ，在匹配update数据进行修改
-- 思考：拉链表如果更新失败，如何补数：从失败内天往后重新计算一遍
insert overwrite table shyl_dim.dim_user_info_orc partition (dt)
select *
from
    (select
         nvl( t2.user_id , t1.user_id )           as user_id
         , nvl( t2.login_name , t1.login_name )   as login_name
         , nvl( t2.nick_name , t1.nick_name )     as nick_name
         , nvl( t2.user_name , t1.user_name )     as user_name
         , nvl( t2.gender , t1.gender )           as gender
         , nvl( t2.birthday , t1.birthday )       as birthday
         , nvl( t2.phone , t1.phone )             as phone
         , nvl( t2.email , t1.email )             as email
         , nvl( t2.user_level , t1.user_level )   as user_level
         , nvl( t2.status , t1.status )           as status
         , t1.create_time
         , t2.operator_time
         , t1.start_time
         , nvl( t2.operator_time , '9999-12-31' ) as end_time
         , ${hivevar:etl_dt}                      as dt
     from
         (select *
          from
              dim_user_info_orc
          where
              dt = date_sub( ${hivevar:etl_dt} , 1 )

          union all

          select
              `data`.id                                              as user_id
              , `data`.login_name                                    as login_name
              , `data`.nick_name                                     as nick_name
              , `data`.name                                          as user_name
              , `data`.birthday                                      as birthday
              , if( `data`.gender is null , '未知' , `data`.gender ) as gender
              , `data`.phone_num                                     as phone
              , `data`.email                                         as email
              , `data`.user_level                                    as user_level
              , `data`.status                                        as status
              , `data`.create_time                                   as create_time
              , `data`.operate_time                                  as operator_time
              , `data`.create_time                                   as start_time
              , '9999-12-31'                                         as end_time
              , dt
          from
              shyl_ods.ods_user_info_inc
          where
              dt = ${hivevar:etl_dt}
              and `type` = 'insert') t1
             left join
             (select
                  `data`.id                                              as user_id
                  , `data`.login_name                                    as login_name
                  , `data`.nick_name                                     as nick_name
                  , `data`.name                                          as user_name
                  , `data`.birthday                                      as birthday
                  , if( `data`.gender is null , '未知' , `data`.gender ) as gender
                  , `data`.phone_num                                     as phone
                  , `data`.email                                         as email
                  , `data`.user_level                                    as user_level
                  , `data`.status                                        as status
                  , `data`.create_time                                   as create_time
                  , `data`.operate_time                                  as operator_time
                  , `data`.create_time                                   as start_time
                  , '9999-12-31'                                         as end_time
                  , dt
              from
                  shyl_ods.ods_user_info_inc
              where
                  dt = ${hivevar:etl_dt}
                  and `type` = 'update') t2
             on t1.user_id = t2.user_id) a
order by
    cast( user_id as int );

-- 数据验证
select
    count( * )
    , `type`
from
    shyl_ods.ods_user_info_inc
where
    dt = '2023-08-19'
group by
    `type`;

-- 200|insert
-- 240|update
-- 所以 19号的数据 + 18号的数据  应该为 400 行
select
    count( * )
    , dt
from
    shyl_dim.dim_user_info_orc
group by
    dt;

-- 200|2023-08-18
-- 400|2023-08-19

-- 商品表 -- 维度应该是sku,来源表 是全量表，所以每天更新当天数据即可
-- 名词解释
-- SPU：Standard Product Unit，标准产品单元，可以理解为一个产品型号，比如上面图片看到的iPhone 14 (A2884) 就是一个标准的产品单元，它属于生产制造过程的一个标准品，
--     标准品在缺乏具体规格信息的时候是不能直接售卖的（除非这个产品系列只有一个规格）。这是商品的基础，包含了商品的一些基础属性，如品牌、型号、公共属性（如手机的屏幕大小、电池容量、支持的网络制式等）等。
--     通过 SPU 我们可以知道某个商品型号的售卖情况；
-- SKU：Stock Keeping Unit，最小库存单元，也就是对应仓库中的一件商品，这个商品的规格信息在入库的时候就已经确定了的，因此是可以直接售卖的。
--     实际售卖的商品，主要是会包含某款商品的特有信息，比如商品缩略图、原价、销售价格、库存数量、优惠政策等等。
-- 平台属性和销售属性：平台属性和销售属性指的都是商品的属性
-- 具体文章参考  https://blog.csdn.net/weixin_43589563/article/details/120856042
-- 平台属性： 属性的作用是帮助我们检索商品，这些属性是电商平台提供的，因此这些属性称之为平台属性
-- 销售属性：当你添加购物车选择不同的 规格 ，版本时，比如手机  ：选择颜色，机身内存，保险服务等等
DROP TABLE IF EXISTS shyl_dim.dim_product_info_orc;
CREATE TABLE if not exists shyl_dim.dim_product_info_orc
(
    id             STRING COMMENT '编号' ,
    sku_id         STRING COMMENT 'SKU编码' ,
    sku_name       STRING COMMENT 'SKU名称' ,
    sku_desc       STRING COMMENT 'sku描述' ,
    spu_id         STRING COMMENT 'SPU编码' ,
    spu_name       STRING COMMENT 'SPU名称' ,
    spu_desc       STRING COMMENT 'spu描述' ,
    price          STRING COMMENT '价格' ,
    brand_id       STRING COMMENT '品牌ID' ,
    brand_name     STRING COMMENT '品牌名称' ,
    category1_id   STRING COMMENT '一级分类ID' ,
    category1_name STRING COMMENT '一级分类名称' ,
    category2_id   STRING COMMENT '二级分类ID' ,
    category2_name STRING COMMENT '二级分类名称' ,
    category3_id   STRING COMMENT '三级分类ID' ,
    category3_name STRING COMMENT '三级分类名称' ,
    is_sale        STRING COMMENT '是否销售' ,
    create_time    STRING COMMENT '上架日期' ,
    attr_value     array<struct<attr_id :string,attr_name :string,value_id :string,value_name
                                :string>> comment '平台属性' ,
    sale_value     array<struct<sale_attr_id :string,sale_attr_name :string,sale_attr_value_id :string,sale_attr_value_name
                                :string>> comment '销售属性' ,
    update_time    STRING COMMENT '修改日期' ,
    end_time       STRING COMMENT '下架日期'
) COMMENT '产品信息全量表'
    partitioned by (dt string comment '分区字段 YYYY-MM-dd')
    stored as orc;

insert overwrite table shyl_dim.dim_product_info_orc partition (dt)
select
    sku.id            as id
    , sku.id          as sku_id
    , sku.sku_name    as sku_name
    , sku.sku_desc    as sku_desc
    , spu.id          as spu_id
    , spu.spu_name    as spu_name
    , spu.description as spu_desc
    , sku.price       as price
    , tm.id           as brand_id
    , tm.tm_name      as brand_name
    , category1.id    as category1_id
    , category1.name  as category1_name
    , category2.id    as category2_id
    , category2.name  as category2_name
    , category3.id    as category3_id
    , category3.name  as category3_name
    , sku.is_sale     as is_sale
    , sku.create_time as create_tima
    , attr.attr_value as attr_value
    , sale.sale_value as sale_value
    , null            as update_time
    , null            as end_time
    , sku.dt          as dt
from
    (select *
     from
         shyl_ods.ods_sku_info_full
     where
         dt = ${hivevar:etl_dt}) sku

        left join

        (select *
         from
             shyl_ods.ods_spu_info_full
         where
             dt = ${hivevar:etl_dt}) spu
        on sku.spu_id = spu.id

        left join

        (select
             id
             , tm_name
         from
             shyl_ods.ods_base_trademark_full
         where
             dt = ${hivevar:etl_dt}) tm
        on sku.tm_id = tm.id

        left join

        (select *
         from
             shyl_ods.ods_base_category3_full
         where
             dt = ${hivevar:etl_dt}) category3
        on spu.category3_id = category3.id

        left join

        (select *
         from
             shyl_ods.ods_base_category2_full
         where
             dt = ${hivevar:etl_dt}) category2
        on category3.category2_id = category2.id

        left join

        (select *
         from
             shyl_ods.ods_base_category1_full
         where
             dt = ${hivevar:etl_dt}) category1
        on category2.category1_id = category1.id

        left join

        (select
             sku_id
             , collect_set( named_struct( "attr_id" , attr_id ,
                                          "attr_name" , attr_name ,
                                          "value_id" , value_id ,
                                          "value_name" , value_name
                ) ) as attr_value
         from
             shyl_ods.ods_sku_attr_value_full
         where
             dt = ${hivevar:etl_dt}
         group by
             sku_id
         order by
             cast( sku_id as int )) attr
        on sku.id = attr.sku_id

        left join

        (select
             sku_id
             , collect_set( named_struct( "sale_attr_id" , sale_attr_id ,
                                          "sale_attr_name" , sale_attr_name ,
                                          "sale_attr_value_id" , sale_attr_value_id ,
                                          "sale_attr_value_name" , sale_attr_value_name
                ) ) as sale_value
         from
             shyl_ods.ods_sku_sale_attr_value_full
         where
             dt = ${hivevar:etl_dt}
         group by
             sku_id
         order by
             cast( sku_id as int )) sale
        on sku.id = sale.sku_id;

-- 地区维度表（一次就行）
DROP TABLE IF EXISTS shyl_dim.dim_area_info_orc;
CREATE TABLE if not exists shyl_dim.dim_area_info_orc
(
    `id`            STRING COMMENT 'id' ,
    `province_name` STRING COMMENT '省市名称' ,
    `area_code`     STRING COMMENT '地区编码' ,
    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用' ,
    `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用' ,
    `region_id`     STRING COMMENT '地区id' ,
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表全量表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dim.dim_area_info_orc partition (dt)
select
    province.id
    , province.name
    , province.area_code
    , province.iso_code
    , province.iso_3166_2
    , region_id
    , region_name
    , dt
from
    (select
         id
         , name
         , region_id
         , area_code
         , iso_code
         , iso_3166_2
         , dt
     from
         shyl_ods.ods_base_province_full
     where
         dt = ${hivevar:etl_dt}) province
        left join
        (select
             id
             , region_name
         from
             shyl_ods.ods_base_region_full
         where
             dt = ${hivevar:etl_dt}) region
        on province.region_id = region.id
order by
    cast( province.id as int );

-- 日期维度表  部分数据通过csv文件【execl文件导出csv格式，注意换行符修改为Linux下的\n】导入（日期，是否节假日，星期，说明），其它数据计算得出
create table if not exists shyl_ods.ods_date_import_csv
(
    `date`     string comment '日期' ,
    weekday    string comment '星期' ,
    is_workday string comment '是否工作日（0：否，1：是）' ,
    desc       string comment '说明'
)
    partitioned by (`year` string comment '年份-分区字段')
--     建表时忘记指定字段分隔符为',',因为上传的文件是csv文件，后续使用语句修改
--     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' -- 指定字段分隔符为逗号
--     LINES TERMINATED BY '\n' -- 指定行分隔符
    stored as textfile;
--     因为csv文件携带表头 ，还需要配置跳过表头
--     TBLPROPERTIES ("skip.header.line.count"="1");

msck repair table shyl_ods.ods_date_import_csv;
show partitions shyl_ods.ods_date_import_csv;
select * from shyl_ods.ods_date_import_csv limit 10;
alter table shyl_ods.ods_date_import_csv set serdeproperties ('field.delim'=',');
ALTER TABLE shyl_ods.ods_date_import_csv SET TBLPROPERTIES ("skip.header.line.count"="1");
show create table shyl_ods.ods_date_import_csv;

insert overwrite table shyl_ods.ods_date_import_csv partition (year)
select
    `date`,
    weekday,
    is_workday,
    desc,
    '2022' as year
from shyl_ods.ods_date_import_csv
limit 10;

select count(*) from shyl_ods.ods_date_import_csv where year='2022';-- 10
select * from shyl_ods.ods_date_import_csv where year='2022';-- 9行
select count(*) from shyl_ods.ods_date_import_csv where year='2023';
-- 两个文件总行数 366 +365 =731 一个文件有表头，一个没有。
-- 结果：hive查询729
-- 删除有表头的文件后 无表头 hive查询364



create table if not exists shyl_dim.dim_date_info_orc
(
    `date`     string comment '日期' ,
    day_id     string comment '一年中的第几天' ,
    `weekday`  string comment '星期' ,
    week_id    string comment '一年中的第几周' ,
    quarter    string comment '一年中的第几季度' ,
    is_workday string comment '是否工作日（0：否，1：是）' ,
    `desc`     string comment '说明'
)
    partitioned by (`year` string comment '年份-分区字段')
    stored as orc;

select
    `date`
    , day( `date` )       as day_id
    , weekday
    , dayofweek( `date` ) as week_id
    , CASE
        WHEN month( `date` ) BETWEEN 1 AND 3   THEN 'Q1'
        WHEN month( `date` ) BETWEEN 4 AND 6   THEN 'Q2'
        WHEN month( `date` ) BETWEEN 7 AND 9   THEN 'Q3'
        WHEN month( `date` ) BETWEEN 10 AND 12 THEN 'Q4'
        END               AS quarter
    , is_workday
    , desc
    , year( `date` )
from
    shyl_ods.ods_date_import_csv
where
    `year` = 2023;







