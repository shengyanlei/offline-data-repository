-- date:2023-09-15
-- author:@shyl
-- 问题：增量表第二天关联时需要去重处理
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
    dt = ${dt}
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
insert overwrite table shyl_dim.dim_user_info_orc partition (dt);
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
    dt = ${dt}
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
    dt = ${dt}
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
    dt = ${dt}
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
    dt = ${dt}
    and type = 'bootstrap-insert';

=============================================================lzo
-- 加工口径
show partitions shyl_ods.ods_user_info_inc;
select *
from
    shyl_ods.ods_user_info_inc
where
    dt = ${dt}
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
    dt = ${dt}
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
         , ${dt}                                  as dt
     from
         (select *
          from
              dim_user_info_orc
          where
              dt = date_sub( ${dt} , 1 )

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
              dt = ${dt}
              and `type` = 'insert') t1
             left join
             (-- 同一个人一天多次修改记录 会造成数据的重复？解决方式:取最新一条
                 select
                     user_id
                     , login_name
                     , nick_name
                     , user_name
                     , birthday
                     , gender
                     , phone
                     , email
                     , user_level
                     , status
                     , create_time
                     , operator_time
                     , start_time
                     , end_time
                     , dt
                     , rn
                 from
                     (select
                          `data`.id                                                      as user_id
                          , `data`.login_name                                            as login_name
                          , `data`.nick_name                                             as nick_name
                          , `data`.name                                                  as user_name
                          , `data`.birthday                                              as birthday
                          , if( `data`.gender is null , '未知' , `data`.gender )         as gender
                          , `data`.phone_num                                             as phone
                          , `data`.email                                                 as email
                          , `data`.user_level                                            as user_level
                          , `data`.status                                                as status
                          , `data`.create_time                                           as create_time
                          , `data`.operate_time                                          as operator_time
                          , `data`.create_time                                           as start_time
                          , '9999-12-31'                                                 as end_time
                          , dt
                          , row_number( ) over (partition by `data`.id order by ts desc) as rn
                      from
                          shyl_ods.ods_user_info_inc
                      where
                          dt = ${dt}
                          and `type` = 'update'
                      order by
                          cast( `data`.id as int )) a
                 where
                     rn = 1) t2
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
         dt = ${dt}) sku

        left join

        (select *
         from
             shyl_ods.ods_spu_info_full
         where
             dt = ${dt}) spu
        on sku.spu_id = spu.id

        left join

        (select
             id
             , tm_name
         from
             shyl_ods.ods_base_trademark_full
         where
             dt = ${dt}) tm
        on sku.tm_id = tm.id

        left join

        (select *
         from
             shyl_ods.ods_base_category3_full
         where
             dt = ${dt}) category3
        on spu.category3_id = category3.id

        left join

        (select *
         from
             shyl_ods.ods_base_category2_full
         where
             dt = ${dt}) category2
        on category3.category2_id = category2.id

        left join

        (select *
         from
             shyl_ods.ods_base_category1_full
         where
             dt = ${dt}) category1
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
             dt = ${dt}
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
             dt = ${dt}
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
         dt = ${dt}) province
        left join
        (select
             id
             , region_name
         from
             shyl_ods.ods_base_region_full
         where
             dt = ${dt}) region
        on province.region_id = region.id
order by
    cast( province.id as int );

-- 日期维度表  部分数据通过csv文件【execl文件导出csv格式，注意换行符修改为Linux下的\n】导入（日期，是否节假日，星期，说明），其它数据计算得出
drop table if exists shyl_ods.ods_date_import_csv;
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

msck repair table shyl_ods.ods_date_import_csv;
show partitions shyl_ods.ods_date_import_csv;

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

insert overwrite table shyl_dim.dim_date_info_orc partition (year)
select
    `date`
    , datediff( `date` , '2022-12-31' ) as day_id
    , weekday
    , dayofweek( `date` )               as week_id
    , CASE
        WHEN month( `date` ) BETWEEN 1 AND 3   THEN 'Q1'
        WHEN month( `date` ) BETWEEN 4 AND 6   THEN 'Q2'
        WHEN month( `date` ) BETWEEN 7 AND 9   THEN 'Q3'
        WHEN month( `date` ) BETWEEN 10 AND 12 THEN 'Q4'
        END                             AS quarter
    , is_workday
    , desc
    , year( `date` )
from
    shyl_ods.ods_date_import_csv
where
    `year` = 2023;

-- 活动信息维度表
drop table if exists shyl_dim.dim_activity_info_orc;
create table if not exists shyl_dim.dim_activity_info_orc
(
    activity_rule_id    string comment '活动规则id' ,
    activity_id         string comment '活动id' ,
    activity_name       string comment '活动名称' ,
    activity_type_code  string comment '活动类型编码' ,
    activitye_type_name string comment '活动类型' ,
    activity_desc       string comment '活动描述' ,
    condition_amount    string comment '满减金额' ,
    condition_num       string comment '满减件数' ,
    benefit_amount      string comment '优惠金额' ,
    benefit_discount    string comment '优惠折扣' ,
    benefit_level       string comment '优惠等级' ,
    benefit_rule        string comment '优惠规则' ,
    start_time          string comment '活动开始时间' ,
    end_time            string comment '活动结束时间' ,
    create_time         string comment '活动创建时间'
)
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dim.dim_activity_info_orc partition (dt)
select
    rule.activity_rule_id
    , act.activity_id
    , act.activity_name
    , act.activity_type as activitye_type_code
    , dic.dic_name      as activitye_type_name
    , act.activity_desc
    , rule.condition_amount
    , rule.condition_num
    , rule.benefit_amount
    , rule.benefit_discount
    , rule.benefit_level
    , case rule.activity_type
        when '3101' then concat( '满' , condition_amount , '元减' , benefit_amount , '元' )
        when '3102' then concat( '满' , condition_num , '件打' , 10 * (1 - benefit_discount) , '折' )
        when '3103' then concat( '打' , 10 * (1 - benefit_discount) , '折' )
        end                benefit_rule
    , act.start_time
    , act.end_time
    , act.create_time
    , act.dt
from
    (select
         id                                                as activity_id
         , activity_name
         , activity_type
         , activity_desc
         , start_time
         , end_time
         , nvl( create_time , date_sub( start_time , 1 ) ) as create_time
         , dt
     from
         shyl_ods.ods_activity_info_full
     where
         dt = ${dt}) act

        left join

        (select
             id as activity_rule_id
             , activity_id
             , activity_type
             , condition_amount
             , condition_num
             , benefit_amount
             , benefit_discount
             , benefit_level
         from
             shyl_ods.ods_activity_rule_full
         where
             dt = ${dt}) rule
        on act.activity_id = rule.activity_id

        left join

        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '31') dic
        on act.activity_type = dic.dic_code;

-- 检验数据
select
    count( * )
    , dt
from
    shyl_dim.dim_activity_info_orc
group by
    dt;

select *
from
    dim_activity_info_orc;

-- 优惠券表 dim_coupon_info_orc
-- 1.解析ods_coupon_use_inc
{"id":null,"coupon_id":null,"user_id":null,"order_id":null,"coupon_status":null,
"get_time":null,"using_time":null,"used_time":null,"expire_time":null}

drop table if exists shyl_dwd.dwd_coupon_use_di;
create table if not exists shyl_dwd.dwd_coupon_use_inc
(
    id                 string comment '编号' ,
    coupon_id          string comment '优惠券id' ,
    user_id            string comment '用户id' ,
    order_id           string comment '订单id' ,
    coupon_status_code string comment '优惠券状态编码' ,
    coupon_status      string comment '优惠券状态' ,
    get_time           string comment '优惠券获取时间' ,
    using_time         string comment '优惠券使用时间' ,
    used_time          string comment '使用优惠券支付时间' ,
    expire_time        string comment '优惠券过期时间'
)
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;
-- 初始第一天
insert overwrite table shyl_dwd.dwd_coupon_use_di partition (dt)
select
    id
    , coupon_id
    , user_id
    , order_id
    , coupon_status as coupon_status_code
    , dic_name      as coupon_status
    , get_time
    , using_time
    , used_time
    , expire_time
    , t1.dt
from
    (select
         `data`.id
         , `data`.coupon_id
         , `data`.user_id
         , `data`.order_id
         , `data`.coupon_status
         , `data`.get_time
         , `data`.using_time
         , `data`.used_time
         , `data`.expire_time
         , dt
     from
         shyl_ods.ods_coupon_use_inc
     where
         `dt` = ${dt}
         and type = 'bootstrap-insert') t1

        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '14') dic
        on t1.coupon_status = dic.dic_code;

-- 之后每天更新
insert overwrite table shyl_dwd.dwd_coupon_use_di partition (dt)
select
    init.id
    , init.coupon_id
    , init.user_id
    , nvl( up.order_id , init.order_id )                     as order_id
    , nvl( up.coupon_status_code , init.coupon_status_code ) as coupon_status_code
    , nvl( up.coupon_status , init.coupon_status )           as coupon_status
    , init.get_time
    , nvl( up.using_time , init.using_time )                 as using_time
    , nvl( up.used_time , init.used_time )                   as used_time
    , nvl( up.expire_time , init.expire_time )               as expire_time
    , ${dt}                                                  as dt
from
    (select *
     from
         shyl_dwd.dwd_coupon_use_di
     where
         dt = date_sub( ${dt} , 1 )
     union
     select
         id
         , coupon_id
         , user_id
         , order_id
         , coupon_status as coupon_status_code
         , dic_name      as coupon_status
         , get_time
         , using_time
         , used_time
         , expire_time
         , t1.dt
     from
         (select
              `data`.id
              , `data`.coupon_id
              , `data`.user_id
              , `data`.order_id
              , `data`.coupon_status
              , `data`.get_time
              , `data`.using_time
              , `data`.used_time
              , `data`.expire_time
              , dt
          from
              shyl_ods.ods_coupon_use_inc
          where
              `dt` = ${dt}
              and type = 'insert') t1

             left join
             (select
                  dic_code
                  , dic_name
              from
                  shyl_ods.ods_base_dic_full
              where
                  dt = ${dt}
                  and parent_code = '14') dic
             on t1.coupon_status = dic.dic_code) init

        left join

        (select
             id
             , coupon_id
             , user_id
             , order_id
             , coupon_status as coupon_status_code
             , dic_name      as coupon_status
             , get_time
             , using_time
             , used_time
             , expire_time
             , t1.dt
         from
             (select
                  `data`.id
                  , `data`.coupon_id
                  , `data`.user_id
                  , `data`.order_id
                  , `data`.coupon_status
                  , `data`.get_time
                  , `data`.using_time
                  , `data`.used_time
                  , `data`.expire_time
                  , dt
              from
                  shyl_ods.ods_coupon_use_inc
              where
                  `dt` = ${dt}
                  and type = 'update') t1

                 left join
                 (select
                      dic_code
                      , dic_name
                  from
                      shyl_ods.ods_base_dic_full
                  where
                      dt = ${dt}
                      and parent_code = '14') dic
                 on t1.coupon_status = dic.dic_code) up
        on init.id = up.id;
-- 查看数据

drop table if exists shyl_dim.dim_coupon_info_orc;
create table if not exists shyl_dim.dim_coupon_info_orc
(
    coupon_id        string comment '购物券id' ,
    coupon_name      string comment '购物券名称' ,
    coupon_type_code string comment '购物券类型编码' ,
    coupon_type_name string comment '购物券类型' ,
    condition_amount string comment '满减金额' ,
    condition_num    string comment '满减件数' ,
    activity_id      string comment '活动id' ,
    benefit_amount   string comment '优惠金额' ,
    benefit_discount string comment '优惠折扣' ,
    benefit_rule     string comment '优惠规则' ,
    create_time      string comment '创建时间' ,
    range_type_code  string comment '范围类型编码' ,
    range_type_name  string comment '范围类型：1.商品 2.品类 3.品牌' ,
    limit_num        string comment '最多领用次数' ,
    taken_count      string comment '已领用次数' ,
    start_time       string comment '开始领取时间' ,
    end_time         string comment '结束领取时间' ,
    operate_time     string comment '修改时间' ,
    expire_time      string comment '过期时间'
) PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dim.dim_coupon_info_orc partition (dt)
select
    coupon_id
    , coupon_name
    , coupon_type         as coupon_type_code
    , coupon_dic.dic_name as coupon_type_name
    , condition_amount
    , condition_num
    , activity_id
    , benefit_amount
    , benefit_discount
    , case coupon_type
        when '3201' then concat( '满' , condition_amount , '元减' , benefit_amount , '元' )
        when '3202' then concat( '满' , condition_num , '件打' , 10 * (1 - benefit_discount) , '折' )
        when '3203' then concat( '减' , benefit_amount , '元' )
        end               as benefit_rule
    , create_time
    , range_type          as range_type_code
    , range_dic.dic_name  as range_type_name
    , limit_num
    , taken_count
    , start_time
    , end_time
    , operate_time
    , expire_time
    , dt
from
    (select
         id as coupon_id
         , coupon_name
         , coupon_type
         , condition_amount
         , condition_num
         , activity_id
         , benefit_amount
         , benefit_discount
         , create_time
         , range_type
         , limit_num
         , taken_count
         , start_time
         , end_time
         , operate_time
         , expire_time
         , dt
     from
         shyl_ods.ods_coupon_info_full
     where
         dt = ${dt}) coupon

        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '32') coupon_dic
        on coupon.coupon_type = coupon_dic.dic_code
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '33') range_dic
        on coupon.range_type = range_dic.dic_code;
















