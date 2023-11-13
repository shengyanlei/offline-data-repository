-- date:2023-09-01
-- author:@shyl
-- 写在前面：
-- 问题：增量表第二天关联时需要去重处理
/**
  测试 业务数据 外部表 还是内部表好
  结论：最源头的数据其实保留在业务数据库中，可以随时拉去，所以内外都可，这里使用外部表
  dwd层的编码实则不应该关联编码表，取得实际值，而应在ads层结果展示时再关联，项目中关联是为了方便理解业务
 */
-- 全量表 "activity_info","activity_rule","base_category1","base_category2","base_category3","base_dic","base_province","base_region"
--         ,"base_trademark","cart_info","coupon_info","coupon_info","sku_attr_value","sku_sale_attr_value","sku_info","spu_info"
drop table if exists shyl_ods.ods_activity_info_full;
CREATE external TABLE shyl_ods.ods_activity_info_full
(
    id            BIGINT COMMENT '活动id' ,
    activity_name STRING COMMENT '活动名称' ,
    activity_type STRING COMMENT '活动类型（1：满减，2：折扣）' ,
    activity_desc STRING COMMENT '活动描述' ,
    start_time    string COMMENT '开始时间' ,
    end_time      string COMMENT '结束时间' ,
    create_time   string COMMENT '创建时间'
)
    COMMENT '活动表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC
    location "/origin_data/gmall/db/activity_info_full";

msck repair table shyl_ods.ods_activity_info_full;
show partitions shyl_ods.ods_activity_info_full;
select *
from
    shyl_ods.ods_activity_info_full
limit 10;

drop table if exists shyl_ods.ods_activity_rule_full;
CREATE external TABLE if not exists shyl_ods.ods_activity_rule_full
(
    id               BIGINT COMMENT '编号' ,
    activity_id      STRING COMMENT '活动id' ,
    activity_type    STRING COMMENT '活动类型（1：满减，2：折扣）' ,
    condition_amount STRING COMMENT '满减金额' ,
    condition_num    bigint COMMENT '满减件数' ,
    benefit_amount   string COMMENT '优惠金额' ,
    benefit_discount string COMMENT '优惠折扣' ,
    benefit_level    bigint comment '优惠级别'
)
    COMMENT '优惠规则表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC
    location "/origin_data/gmall/db/activity_rule_full";

msck repair table shyl_ods.ods_activity_rule_full;
show partitions shyl_ods.ods_activity_rule_full;
select *
from
    shyl_ods.ods_activity_rule_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_category1_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_category1_full
(
    `id`   STRING COMMENT '编号' ,
    `name` STRING COMMENT '分类名称'
) COMMENT '一级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_category1_full/';

msck repair table shyl_ods.ods_base_category1_full;
show partitions shyl_ods.ods_base_category1_full;
select *
from
    shyl_ods.ods_base_category1_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_category2_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_category2_full
(
    `id`           STRING COMMENT '编号' ,
    `name`         STRING COMMENT '二级分类名称' ,
    `category1_id` STRING COMMENT '一级分类编号'
) COMMENT '二级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_category2_full/';

msck repair table shyl_ods.ods_base_category2_full;
show partitions shyl_ods.ods_base_category2_full;
select *
from
    shyl_ods.ods_base_category2_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_category3_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_category3_full
(
    `id`           STRING COMMENT '编号' ,
    `name`         STRING COMMENT '三级分类名称' ,
    `category2_id` STRING COMMENT '二级分类编号'
) COMMENT '三级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_category3_full/';

msck repair table shyl_ods.ods_base_category3_full;
show partitions shyl_ods.ods_base_category3_full;
select *
from
    shyl_ods.ods_base_category3_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_dic_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号' ,
    `dic_name`     STRING COMMENT '编码名称' ,
    `parent_code`  STRING COMMENT '父编号' ,
    `create_time`  STRING COMMENT '创建日期' ,
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_dic_full/';

msck repair table shyl_ods.ods_base_dic_full;
show partitions shyl_ods.ods_base_dic_full;
select *
from
    shyl_ods.ods_base_dic_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_province_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_province_full
(
    `id`         STRING COMMENT '编号' ,
    `name`       STRING COMMENT '省份名称' ,
    `region_id`  STRING COMMENT '地区ID' ,
    `area_code`  STRING COMMENT '地区编码' ,
    `iso_code`   STRING COMMENT '旧版ISO-3166-2编码，供可视化使用' ,
    `iso_3166_2` STRING COMMENT '新版IOS-3166-2编码，供可视化使用'
) COMMENT '省份表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_province_full/';

msck repair table shyl_ods.ods_base_province_full;
show partitions shyl_ods.ods_base_province_full;
select *
from
    shyl_ods.ods_base_province_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_region_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_region_full
(
    `id`          STRING COMMENT '编号' ,
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_region_full/';

msck repair table shyl_ods.ods_base_region_full;
show partitions shyl_ods.ods_base_region_full;
select *
from
    shyl_ods.ods_base_region_full;

DROP TABLE IF EXISTS shyl_ods.ods_base_trademark_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_trademark_full
(
    `id`       STRING COMMENT '编号' ,
    `tm_name`  STRING COMMENT '品牌名称' ,
    `logo_url` STRING COMMENT '品牌logo的图片路径'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_trademark_full/';

msck repair table shyl_ods.ods_base_trademark_full;
show partitions shyl_ods.ods_base_trademark_full;
select *
from
    shyl_ods.ods_base_trademark_full;

DROP TABLE IF EXISTS shyl_ods.ods_cart_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_cart_info_full
(
    `id`           STRING COMMENT '编号' ,
    `user_id`      STRING COMMENT '用户id' ,
    `sku_id`       STRING COMMENT 'sku_id' ,
    `cart_price`   DECIMAL(16 , 2) COMMENT '放入购物车时价格' ,
    `sku_num`      BIGINT COMMENT '数量' ,
    `img_url`      BIGINT COMMENT '商品图片地址' ,
    `sku_name`     STRING COMMENT 'sku名称 (冗余)' ,
    `is_checked`   STRING COMMENT '是否被选中' ,
    `create_time`  STRING COMMENT '创建时间' ,
    `operate_time` STRING COMMENT '修改时间' ,
    `is_ordered`   STRING COMMENT '是否已经下单' ,
    `order_time`   STRING COMMENT '下单时间' ,
    `source_type`  STRING COMMENT '来源类型' ,
    `source_id`    STRING COMMENT '来源编号'
) COMMENT '购物车全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/cart_info_full/';

msck repair table shyl_ods.ods_cart_info_full;
show partitions shyl_ods.ods_cart_info_full;
select *
from
    shyl_ods.ods_cart_info_full;

DROP TABLE IF EXISTS shyl_ods.ods_coupon_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_coupon_info_full
(
    `id`               STRING COMMENT '购物券编号' ,
    `coupon_name`      STRING COMMENT '购物券名称' ,
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券' ,
    `condition_amount` DECIMAL(16 , 2) COMMENT '满额数' ,
    `condition_num`    BIGINT COMMENT '满件数' ,
    `activity_id`      STRING COMMENT '活动编号' ,
    `benefit_amount`   DECIMAL(16 , 2) COMMENT '减金额' ,
    `benefit_discount` DECIMAL(16 , 2) COMMENT '折扣' ,
    `create_time`      STRING COMMENT '创建时间' ,
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌' ,
    `limit_num`        BIGINT COMMENT '最多领用次数' ,
    `taken_count`      BIGINT COMMENT '已领用次数' ,
    `start_time`       STRING COMMENT '开始领取时间' ,
    `end_time`         STRING COMMENT '结束领取时间' ,
    `operate_time`     STRING COMMENT '修改时间' ,
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/coupon_info_full/';

msck repair table shyl_ods.ods_coupon_info_full;
show partitions shyl_ods.ods_coupon_info_full;
select *
from
    shyl_ods.ods_coupon_info_full;

DROP TABLE IF EXISTS shyl_ods.ods_sku_attr_value_full;
CREATE EXTERNAL TABLE shyl_ods.ods_sku_attr_value_full
(
    `id`         STRING COMMENT '编号' ,
    `attr_id`    STRING COMMENT '平台属性ID' ,
    `value_id`   STRING COMMENT '平台属性值ID' ,
    `sku_id`     STRING COMMENT '商品ID' ,
    `attr_name`  STRING COMMENT '平台属性名称' ,
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/sku_attr_value_full/';

msck repair table shyl_ods.ods_sku_attr_value_full;
show partitions shyl_ods.ods_sku_attr_value_full;
select *
from
    shyl_ods.ods_sku_attr_value_full;

DROP TABLE IF EXISTS shyl_ods.ods_sku_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_sku_info_full
(
    `id`              STRING COMMENT 'skuId' ,
    `spu_id`          STRING COMMENT 'spuid' ,
    `price`           DECIMAL(16 , 2) COMMENT '价格' ,
    `sku_name`        STRING COMMENT '商品名称' ,
    `sku_desc`        STRING COMMENT '商品描述' ,
    `weight`          DECIMAL(16 , 2) COMMENT '重量' ,
    `tm_id`           STRING COMMENT '品牌id' ,
    `category3_id`    STRING COMMENT '品类id' ,
    `sku_default_igm` STRING COMMENT '商品图片地址' ,
    `is_sale`         STRING COMMENT '是否在售' ,
    `create_time`     STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/sku_info_full/';

msck repair table shyl_ods.ods_sku_info_full;
show partitions shyl_ods.ods_sku_info_full;
select *
from
    shyl_ods.ods_sku_info_full;

DROP TABLE IF EXISTS shyl_ods.ods_sku_sale_attr_value_full;
CREATE EXTERNAL TABLE shyl_ods.ods_sku_sale_attr_value_full
(
    `id`                   STRING COMMENT '编号' ,
    `sku_id`               STRING COMMENT 'sku_id' ,
    `spu_id`               STRING COMMENT 'spu_id' ,
    `sale_attr_value_id`   STRING COMMENT '销售属性值id' ,
    `sale_attr_id`         STRING COMMENT '销售属性id' ,
    `sale_attr_name`       STRING COMMENT '销售属性名称' ,
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/sku_sale_attr_value_full/';

msck repair table shyl_ods.ods_sku_sale_attr_value_full;
show partitions shyl_ods.ods_sku_sale_attr_value_full;
select *
from
    shyl_ods.ods_sku_sale_attr_value_full;

DROP TABLE IF EXISTS shyl_ods.ods_spu_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_spu_info_full
(
    `id`           STRING COMMENT 'spu_id' ,
    `spu_name`     STRING COMMENT 'spu名称' ,
    `description`  STRING COMMENT '描述信息' ,
    `category3_id` STRING COMMENT '品类id' ,
    `tm_id`        STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/spu_info_full/';

msck repair table shyl_ods.ods_spu_info_full;
show partitions shyl_ods.ods_spu_info_full;
select *
from
    shyl_ods.ods_spu_info_full;



--增量表
-- ###########################################################################################
-- 没用的表
-- ###########################################################################################
DROP TABLE IF EXISTS shyl_ods.ods_activity_sku_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_activity_sku_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,activity_id:bigint,sku_id:bigint,create_time:string> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '活动参与商品增量表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/activity_sku_inc';
--
-- msck repair  table shyl_ods.ods_activity_sku_inc;
-- show partitions shyl_ods.ods_activity_sku_inc;
-- select * from shyl_ods.ods_activity_sku_inc;
--
DROP TABLE IF EXISTS shyl_ods.ods_base_attr_info_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_base_attr_info_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,attr_name:string,category_id:bigint,category_level:int> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '属性增量表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/base_attr_info_inc';
--
-- msck repair table shyl_ods.ods_base_attr_info_inc;
-- show partitions shyl_ods.ods_base_attr_info_inc;
-- select * from shyl_ods.ods_base_attr_info_inc;
--
DROP TABLE IF EXISTS shyl_ods.ods_base_attr_value_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_base_attr_value_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,value_name:string,attr_id:bigint> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '属性增量表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/base_attr_value_inc';
--
-- msck repair table shyl_ods.ods_base_attr_value_inc;
-- show partitions shyl_ods.ods_base_attr_value_inc;
-- select * from shyl_ods.ods_base_attr_value_inc;
--
DROP TABLE IF EXISTS shyl_ods.ods_base_frontend_param_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_base_frontend_param_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,code:string,delete_id:bigint> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '前端数据保护表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/base_frontend_param_inc';
--
-- msck repair table shyl_ods.ods_base_frontend_param_inc;
-- show partitions shyl_ods.ods_base_frontend_param_inc;
-- select * from shyl_ods.ods_base_frontend_param_inc;
--
DROP TABLE IF EXISTS shyl_ods.ods_base_sale_attr_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_base_sale_attr_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,name:string> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '基本销售属性表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/base_sale_attr_inc';
--
-- msck repair table shyl_ods.ods_base_sale_attr_inc;
-- show partitions shyl_ods.ods_base_sale_attr_inc;
-- select * from shyl_ods.ods_base_sale_attr_inc;
--
--
DROP TABLE IF EXISTS shyl_ods.ods_cms_banner_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_cms_banner_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,title:string,image_url:string,link_url:string,sort:int> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '首页banner表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/cms_banner_inc';
--
-- msck repair table shyl_ods.ods_cms_banner_inc;
-- show partitions shyl_ods.ods_cms_banner_inc;
-- select * from shyl_ods.ods_cms_banner_inc;
--
DROP TABLE IF EXISTS shyl_ods.ods_coupon_range_inc;
-- CREATE EXTERNAL TABLE shyl_ods.ods_coupon_range_inc
-- (
--     `type` STRING COMMENT '变动类型',
--     `ts`   BIGINT COMMENT '变动时间',
--     `data` STRUCT<id :bigint,coupon_id:bigint,range_type:string,range_id:bigint> COMMENT '数据',
--     `old` map<string,string>
-- ) COMMENT '优惠券范围表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
--     LOCATION '/origin_data/gmall/db/coupon_range_inc';
--
-- msck repair table shyl_ods.ods_coupon_range_inc;
-- show partitions shyl_ods.ods_coupon_range_inc;
-- select * from shyl_ods.ods_coupon_range_inc;
-- #################################################################################################
待定
DROP TABLE IF EXISTS shyl_ods.ods_cart_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_cart_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,cart_price :DECIMAL(16 , 2),sku_num :BIGINT,img_url :STRING,sku_name
                  :STRING,is_checked :STRING,create_time :STRING,operate_time :STRING,is_ordered :STRING,order_time
                  :STRING,source_type :STRING,source_id :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '购物车增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/cart_info_inc/';

msck repair table shyl_ods.ods_cart_info_inc;
show partitions shyl_ods.ods_cart_info_inc;
select *
from
    shyl_ods.ods_cart_info_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_comment_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_comment_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,user_id :STRING,nick_name :STRING,head_img :STRING,sku_id :STRING,spu_id :STRING,order_id
                  :STRING,appraise :STRING,comment_txt :STRING,create_time :STRING,operate_time
                  :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '评价表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/comment_info_inc/';

msck repair table shyl_ods.ods_comment_info_inc;
show partitions shyl_ods.ods_comment_info_inc;
select *
from
    shyl_ods.ods_comment_info_inc
limit 10;


DROP TABLE IF EXISTS shyl_ods.ods_coupon_use_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_coupon_use_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,coupon_id :STRING,user_id :STRING,order_id :STRING,coupon_status :STRING,get_time :STRING,using_time
                  :STRING,used_time :STRING,expire_time :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/coupon_use_inc/';

msck repair table shyl_ods.ods_coupon_use_inc;
show partitions shyl_ods.ods_coupon_use_inc;
select *
from
    shyl_ods.ods_coupon_use_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_favor_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_favor_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,spu_id :STRING,is_cancel :STRING,create_time :STRING,cancel_time
                  :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/favor_info_inc/';

msck repair table shyl_ods.ods_favor_info_inc;
show partitions shyl_ods.ods_favor_info_inc;
select *
from
    shyl_ods.ods_favor_info_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_detail_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_detail_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,order_id :STRING,sku_id :STRING,sku_name :STRING,img_url :STRING,order_price
                  :DECIMAL(16 , 2),sku_num :BIGINT,create_time :STRING,source_type :STRING,source_id :STRING,split_total_amount
                  :DECIMAL(16 , 2),split_activity_amount :DECIMAL(16 , 2),split_coupon_amount
                  :DECIMAL(16 , 2)> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_detail_inc/';

msck repair table shyl_ods.ods_order_detail_inc;
show partitions shyl_ods.ods_order_detail_inc;
select *
from
    shyl_ods.ods_order_detail_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_detail_activity_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_detail_activity_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,activity_id :STRING,activity_rule_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细活动关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_detail_activity_inc/';

msck repair table shyl_ods.ods_order_detail_activity_inc;
show partitions shyl_ods.ods_order_detail_activity_inc;
select *
from
    shyl_ods.ods_order_detail_activity_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_detail_coupon_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_detail_coupon_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,coupon_id :STRING,coupon_use_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细优惠券关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_detail_coupon_inc/';

msck repair table shyl_ods.ods_order_detail_coupon_inc;
show partitions shyl_ods.ods_order_detail_coupon_inc;
select *
from
    shyl_ods.ods_order_detail_coupon_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,consignee :STRING,consignee_tel :STRING,total_amount :DECIMAL(16 , 2),order_status :STRING,user_id
                  :STRING,payment_way :STRING,delivery_address :STRING,order_comment :STRING,out_trade_no :STRING,trade_body
                  :STRING,create_time :STRING,operate_time :STRING,expire_time :STRING,process_status :STRING,tracking_no
                  :STRING,parent_order_id :STRING,img_url :STRING,province_id :STRING,activity_reduce_amount
                  :DECIMAL(16 , 2),coupon_reduce_amount :DECIMAL(16 , 2),original_total_amount :DECIMAL(16 , 2),freight_fee
                  :DECIMAL(16 , 2),freight_fee_reduce :DECIMAL(16 , 2),refundable_time
                  :DECIMAL(16 , 2)> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_info_inc/';

msck repair table shyl_ods.ods_order_info_inc;
show partitions shyl_ods.ods_order_info_inc;
select *
from
    shyl_ods.ods_order_info_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_refund_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_refund_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                  :DECIMAL(16 , 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                  :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_refund_info_inc/';

msck repair table shyl_ods.ods_order_refund_info_inc;
show partitions shyl_ods.ods_order_refund_info_inc;
select *
from
    shyl_ods.ods_order_refund_info_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_status_log_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_status_log_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                  :DECIMAL(16 , 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                  :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单状态流水表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_status_log_inc/';

msck repair table shyl_ods.ods_order_status_log_inc;
show partitions shyl_ods.ods_order_status_log_inc;
select *
from
    shyl_ods.ods_order_status_log_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_payment_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_payment_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,user_id :STRING,payment_type :STRING,trade_no
                  :STRING,total_amount :DECIMAL(16 , 2),subject :STRING,payment_status :STRING,create_time :STRING,callback_time
                  :STRING,callback_content :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/payment_info_inc/';

msck repair table shyl_ods.ods_payment_info_inc;
show partitions shyl_ods.ods_payment_info_inc;
select *
from
    shyl_ods.ods_payment_info_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_refund_payment_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_refund_payment_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,sku_id :STRING,payment_type :STRING,trade_no :STRING,total_amount
                  :DECIMAL(16 , 2),subject :STRING,refund_status :STRING,create_time :STRING,callback_time :STRING,callback_content
                  :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退款表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/refund_payment_inc/';

msck repair table shyl_ods.ods_refund_payment_inc;
show partitions shyl_ods.ods_refund_payment_inc;
select *
from
    shyl_ods.ods_refund_payment_inc
limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_user_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_user_info_inc
(
    `type` STRING COMMENT '变动类型' ,
    `ts`   BIGINT COMMENT '变动时间' ,
    `data` STRUCT<id :STRING,login_name :STRING,nick_name :STRING,passwd :STRING,name :STRING,phone_num :STRING,email
                  :STRING,head_img :STRING,user_level :STRING,birthday :STRING,gender :STRING,create_time :STRING,operate_time
                  :STRING,status :STRING> COMMENT '数据' ,
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/user_info_inc/';

msck repair table shyl_ods.ods_user_info_inc;
show partitions shyl_ods.ods_user_info_inc;
select *
from
    shyl_ods.ods_user_info_inc
where
    dt = '2023-08-21';


/**
  业务数据dwd层表设计：
事务事实表：各业务过程的原子操作事件
周期快照事实表：周期快照事实表以具有规律性的、可预见的时间间隔来记录事实，主要用于分析一些存量型或者状态型指标
累计快照事实表：基于一个业务流程中的多个关键业务过程联合处理而构建的事实表
 */
--  购物车加购表 ：记录用户加购商品的操作，粒度：一次加购操作[增加数量也算加购操作]
-- 【注】：业务系统的购物车表是结果表，第一天没有办法确定操作，默认第一天取当时全量表；
set hive.exec.dynamic.partition=true;
-- 使用非严格模式，允许动态分区。
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists shyl_dwd.dwd_trade_cart_add_di;
create table if not exists shyl_dwd.dwd_trade_cart_add_di
(
    id               string comment '编号' ,
    user_id          string comment '用户id' ,
    sku_id           string comment 'sku——id' ,
    cart_pirce       string comment '加入购物车时价格' ,
    sku_num          string comment 'sku数量' ,
    sku_name         string comment 'sku名称' ,
    add_cart_time    string comment '添加购物车时间' ,
    source_id        string comment '来源类型id' ,
    source_type_code string comment '来源类型编码' ,
    source_type_name string comment '来源类型'
) PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dwd.dwd_trade_cart_add_di partition (dt)
select
    id
    , user_id
    , sku_id
    , cart_pirce
    , sku_num
    , sku_name
    , add_cart_time
    , source_id
    , source_type_code
    , dic.dic_name as source_type_name
    , dt
from
    (select
         `data`.id            as id
         , `data`.user_id     as user_id
         , `data`.sku_id      as sku_id
         , `data`.cart_price  as cart_pirce
         , `data`.sku_num     as sku_num
         , `data`.sku_name    as sku_name
         , `data`.create_time as add_cart_time
         , `data`.source_type as source_type_code
         , `data`.source_id   as source_id
         , ${dt}              as dt
     from
         shyl_ods.ods_cart_info_inc
     where
                 dt = ${dt}
             and `type` in ( 'bootstrap-insert' , 'insert' )
         or (dt = ${dt} and `type` = 'update' and old['sku_num'] is not null and
             `data`.sku_num > cast( old['sku_num'] as int ))) cart -- 第二天 新插入的数据或者sku_num变大的购物车

        left join

        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '24') dic
        on cart.source_type_code = dic.dic_code;


-- 订单事实表
-- {"id":null,"consignee":null,"consignee_tel":null,"total_amount":null,"order_status":null,"user_id":null,"payment_way":null,
-- "delivery_address":null,"order_comment":null,"out_trade_no":null,"trade_body":null,"create_time":null,"operate_time":null,
-- "expire_time":null,"process_status":null,"tracking_no":null,"parent_order_id":null,"img_url":null,"province_id":null,
-- "activity_reduce_amount":null,"coupon_reduce_amount":null,"original_total_amount":null,"freight_fee":null,"freight_fee_reduce":null,
-- "refundable_time":null}

drop table if exists shyl_dwd.dwd_trade_order_di;
create table if not exists shyl_dwd.dwd_trade_order_di
(
    order_id              string comment '编号' ,
    sku_id                string comment 'sku_id' ,
    sku_name              string comment 'sku名称' ,
    sku_num               int comment 'sku 数量' ,
    user_id               string comment '用户id' ,
    province_id           string comment '地区id' ,
    activity_id           string comment '活动id' ,
    activity_rule_id      STRING COMMENT '参与活动规则id' ,
    coupon_id             STRING COMMENT '使用优惠券id' ,
    source_id             STRING COMMENT '来源编号' ,
    source_type_code      STRING COMMENT '来源类型编码' ,
    source_type_name      STRING COMMENT '来源类型名称' ,
    create_time           string comment '创建时间' ,
    split_original_amount DECIMAL(16 , 2) COMMENT '原始价格' ,
    split_activity_amount DECIMAL(16 , 2) COMMENT '活动优惠分摊' ,
    split_coupon_amount   DECIMAL(16 , 2) COMMENT '优惠券优惠分摊' ,
    split_total_amount    DECIMAL(16 , 2) COMMENT '最终价格分摊'
) PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;


-- 生产环境下第一天和后续新增分开写，这里是模拟数据 ，第一天不会存在 type为insert的数据
select
    `type`
from
    shyl_ods.ods_order_info_inc
where
    dt = ${dt}
group by
    `type`;

insert overwrite table shyl_dwd.dwd_trade_order_di partition (dt)
select
    `detail`.order_id
    , sku_id
    , sku_name
    , sku_num
    , user_id
    , province_id
    , activity_id
    , activity_rule_id
    , coupon_id
    , source_id
    , source_type as source_type_code
    , dic_name    as source_type_name
    , create_time
    , split_original_amount
    , split_activity_amount
    , split_coupon_amount
    , split_total_amount
    , ${dt}
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.sku_name
         , `data`.sku_num
         , `data`.create_time
         , `data`.source_id
         , `data`.source_type
         , `data`.sku_num * `data`.order_price as split_original_amount
         , `data`.split_activity_amount
         , `data`.split_coupon_amount
         , `data`.split_total_amount
     from
         shyl_ods.ods_order_detail_inc
     where
         dt = ${dt}
         and type in ( 'bootstrap-insert' , 'insert' )) `detail`
        left join (select
                       `data`.id as order_id
                       , `data`.user_id
                       , `data`.province_id
                   from
                       shyl_ods.ods_order_info_inc
                   where
                       dt = ${dt}
                       and type in ( 'bootstrap-insert' , 'insert' )) info
                  on `detail`.order_id = info.order_id
        left join(select
                      `data`.order_detail_id
                      , `data`.activity_id
                      , `data`.activity_rule_id
                  from
                      shyl_ods.ods_order_detail_activity_inc
                  where
                      dt = ${dt}
                      and type in ( 'bootstrap-insert' , 'insert' )) activity
                 on `detail`.id = activity.order_detail_id
        left join(select
                      `data`.order_detail_id
                      , `data`.coupon_id
                  from
                      shyl_ods.ods_order_detail_coupon_inc
                  where
                      dt = ${dt}
                      and type in ( 'bootstrap-insert' , 'insert' )) coupon
                 on `detail`.id = coupon.order_detail_id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '24') dic
        on `detail`.source_type = dic.dic_code;

-- dwd_trade_order_cancel_di
-- 业务过程：取消订单
drop table if exists shyl_dwd.dwd_trade_order_cancel_di;
create table if not exists shyl_dwd.dwd_trade_order_cancel_di
(
    order_id              string comment '订单id' ,
    user_id               string comment '用户id' ,
    sku_id                string comment 'sku_id' ,
    sku_name              string comment 'sku名称' ,
    sku_num               int comment 'sku数量' ,
    province_id           string comment '地区id' ,
    coupon_id             string comment '优惠券id' ,
    activity_id           string comment '活动id' ,
    activity_rule_id      string comment '活动规则id' ,
    create_time           string comment '创建时间' ,
    cancel_time           string comment '取消时间' ,
    cancel_type           string comment '取消类型' ,
    source_id             STRING COMMENT '来源编号' ,
    source_type_code      STRING COMMENT '来源类型编码' ,
    source_type_name      STRING COMMENT '来源类型名称' ,
    split_original_amount DECIMAL(16 , 2) COMMENT '原始价格' ,
    split_activity_amount DECIMAL(16 , 2) COMMENT '活动优惠分摊' ,
    split_coupon_amount   DECIMAL(16 , 2) COMMENT '优惠券优惠分摊' ,
    split_total_amount    DECIMAL(16 , 2) COMMENT '最终价格分摊'
) PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

-- 首日[模拟数据：不需要考虑bootstrap-insert当天还有后续数据产生]
select distinct
    `type`
from
    shyl_ods.ods_order_detail_inc
where
    dt = ${dt};
-- | type |
-- | bootstrap-complete |
-- | bootstrap-insert |
-- | bootstrap-start |
insert overwrite table shyl_dwd.dwd_trade_order_cancel_di partition (dt)
select
    `detail`.order_id
    , user_id
    , sku_id
    , sku_name
    , sku_num
    , province_id
    , coupon_id
    , activity_id
    , activity_rule_id
    , create_time
    , cancel_time
    , ''          as cancel_type
    , source_id
    , source_type as source_type_code
    , dic_name    as source_type_name
    , split_original_amount
    , split_activity_amount
    , split_coupon_amount
    , split_total_amount
    , ${dt}
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.sku_name
         , `data`.sku_num
         , `data`.source_id
         , `data`.source_type
         , `data`.sku_num * `data`.order_price as split_original_amount
         , `data`.split_total_amount
         , `data`.split_activity_amount
         , `data`.split_coupon_amount
     from
         shyl_ods.ods_order_detail_inc
     where
         `type` = 'bootstrap-insert'
         and dt = ${dt}) `detail`
        join
        (select
             `data`.id
             , `data`.user_id
             , `data`.order_status
             , `data`.province_id
             , `data`.create_time
             , `data`.operate_time as cancel_time
         from
             shyl_ods.ods_order_info_inc
         where
             type = 'bootstrap-insert'
             and `data`.order_status = '1003'
             and dt = ${dt}) `order`
        on `detail`.order_id = `order`.id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '24') base
        on `detail`.source_type = `base`.dic_code
        left join
        (select
             `data`.order_detail_id
             , `data`.activity_id
             , `data`.activity_rule_id
         from
             shyl_ods.ods_order_detail_activity_inc
         where
             dt = ${dt}
             and `type` = 'bootstrap-insert') `activity`
        on `detail`.id = `activity`.order_detail_id
        left join
        (select
             `data`.order_detail_id
             , `data`.coupon_id
         from
             shyl_ods.ods_order_detail_coupon_inc
         where
             dt = ${dt}
             and `type` = 'bootstrap-insert') coupon
        on `detail`.id = `coupon`.order_detail_id;

-- 后续
insert overwrite table shyl_dwd.dwd_trade_order_cancel_di partition (dt)
select
    `detail`.order_id
    , user_id
    , sku_id
    , sku_name
    , sku_num
    , province_id
    , coupon_id
    , activity_id
    , activity_rule_id
    , create_time
    , cancel_time
    , ''          as cancel_type
    , source_id
    , source_type as source_type_code
    , dic_name    as source_type_name
    , split_original_amount
    , split_activity_amount
    , split_coupon_amount
    , split_total_amount
    , ${dt}
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.sku_name
         , `data`.sku_num
         , `data`.source_id
         , `data`.source_type
         , `data`.sku_num * `data`.order_price as split_original_amount
         , `data`.split_total_amount
         , `data`.split_activity_amount
         , `data`.split_coupon_amount
     from
         shyl_ods.ods_order_detail_inc
     where
         `type` in ( 'insert' , 'bootstrap-insert' )
         and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) `detail`
        join
        (select
             `data`.id
             , `data`.user_id
             , `data`.order_status
             , `data`.province_id
             , `data`.create_time
             , `data`.operate_time as cancel_time
         from
             shyl_ods.ods_order_info_inc
         where
             type = 'update'
             and dt = ${dt}
             and `data`.order_status = '1003') `order`
        on `detail`.order_id = `order`.id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '24') base
        on `detail`.source_type = `base`.dic_code
        left join
        (select
             `data`.order_detail_id
             , `data`.activity_id
             , `data`.activity_rule_id
         from
             shyl_ods.ods_order_detail_activity_inc
         where
             `type` in ( 'insert' , 'bootstrap-insert' )
             and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) `activity`
        on `detail`.id = `activity`.order_detail_id
        left join
        (select
             `data`.order_detail_id
             , `data`.coupon_id
         from
             shyl_ods.ods_order_detail_coupon_inc
         where
             `type` in ( 'insert' , 'bootstrap-insert' )
             and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) coupon
        on `detail`.id = `coupon`.order_detail_id;

-- 订单成功支付表
drop table if exists shyl_dwd.dwd_trade_order_pay_success_di;
create table if not exists shyl_dwd.dwd_trade_order_pay_success_di
(
    order_id              string comment '订单id' ,
    user_id               string comment '用户id' ,
    sku_id                string comment 'sku_id' ,
    sku_name              string comment 'sku名称' ,
    sku_num               int comment 'sku数量' ,
    province_id           string comment '地区id' ,
    coupon_id             string comment '优惠券id' ,
    activity_id           string comment '活动id' ,
    activity_rule_id      string comment '活动规则id' ,
    create_time           string comment '创建时间' ,
    pay_success_time      string comment '支付成功时间' ,
    pay_type_code         string comment '支付类型编码' ,
    pay_type_name         string comment '支付类型名称' ,
    source_id             STRING COMMENT '来源编号' ,
    source_type_code      STRING COMMENT '来源类型编码' ,
    source_type_name      STRING COMMENT '来源类型名称' ,
    split_original_amount DECIMAL(16 , 2) COMMENT '原始价格' ,
    split_activity_amount DECIMAL(16 , 2) COMMENT '活动优惠分摊' ,
    split_coupon_amount   DECIMAL(16 , 2) COMMENT '优惠券优惠分摊' ,
    split_total_amount    DECIMAL(16 , 2) COMMENT '最终价格分摊'
) PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

-- 第一天
insert overwrite table shyl_dwd.dwd_trade_order_pay_success_di partition (dt)
select
    `detail`.order_id
    , user_id
    , sku_id
    , sku_name
    , sku_num
    , province_id
    , coupon_id
    , activity_id
    , activity_rule_id
    , create_time
    , pay_success_time
    , pay_type_code
    , pay_type_name
    , source_id
    , source_type as source_type_code
    , dic_name    as source_type_name
    , split_original_amount
    , split_activity_amount
    , split_coupon_amount
    , split_total_amount
    , ${dt}
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.sku_name
         , `data`.sku_num
         , `data`.source_id
         , `data`.source_type
         , `data`.sku_num * `data`.order_price as split_original_amount
         , `data`.split_total_amount
         , `data`.split_activity_amount
         , `data`.split_coupon_amount
     from
         shyl_ods.ods_order_detail_inc
     where
         `type` = 'bootstrap-insert'
         and dt = ${dt}) `detail`
        left join
        (select
             `data`.id
             , `data`.user_id
             , `data`.province_id
             , `data`.create_time
         from
             shyl_ods.ods_order_info_inc
         where
             type = 'bootstrap-insert'
             and dt = ${dt}) `order`
        on `detail`.order_id = `order`.id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '24') base
        on `detail`.source_type = `base`.dic_code
        left join
        (select
             `data`.order_detail_id
             , `data`.activity_id
             , `data`.activity_rule_id
         from
             shyl_ods.ods_order_detail_activity_inc
         where
             dt = ${dt}
             and `type` = 'bootstrap-insert') `activity`
        on `detail`.id = `activity`.order_detail_id
        left join
        (select
             `data`.order_detail_id
             , `data`.coupon_id
         from
             shyl_ods.ods_order_detail_coupon_inc
         where
             dt = ${dt}
             and `type` = 'bootstrap-insert') coupon
        on `detail`.id = `coupon`.order_detail_id
        join
        (select
             order_id
             , payment_type  as pay_type_code
             , dic_name      as pay_type_name
             , callback_time as pay_success_time
         from
             (select
                  `data`.order_id
                  , `data`.payment_type
                  , `data`.callback_time
              from
                  shyl_ods.ods_payment_info_inc
              where
                  dt = ${dt}
                  and `type` = 'bootstrap-insert'
                  and `data`.payment_status = '1602') payment
                 left join (select
                                dic_code
                                , dic_name
                            from
                                shyl_ods.ods_base_dic_full
                            where
                                dt = ${dt}
                                and parent_code = '11') base
                           on payment.payment_type = base.dic_code) pay
        on `detail`.order_id = pay.order_id;

-- 后续：
select distinct
    `type`
from
    shyl_ods.ods_payment_info_inc
where
    dt = ${dt};
insert overwrite table shyl_dwd.dwd_trade_order_pay_success_di partition (dt)
select
    `detail`.order_id
    , user_id
    , sku_id
    , sku_name
    , sku_num
    , province_id
    , coupon_id
    , activity_id
    , activity_rule_id
    , create_time
    , pay_success_time
    , pay_type_code
    , pay_type_name
    , source_id
    , source_type as source_type_code
    , dic_name    as source_type_name
    , split_original_amount
    , split_activity_amount
    , split_coupon_amount
    , split_total_amount
    , ${dt}
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.sku_name
         , `data`.sku_num
         , `data`.source_id
         , `data`.source_type
         , `data`.sku_num * `data`.order_price as split_original_amount
         , `data`.split_total_amount
         , `data`.split_activity_amount
         , `data`.split_coupon_amount
     from
         shyl_ods.ods_order_detail_inc
     where
         `type` in ( 'bootstrap-insert' , 'insert' )
         and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) `detail`
        left join
        (select
             `data`.id
             , `data`.user_id
             , `data`.province_id
             , `data`.create_time
         from
             shyl_ods.ods_order_info_inc
         where
             `type` in ( 'bootstrap-insert' , 'insert' )
             and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) `order`
        on `detail`.order_id = `order`.id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '24') base
        on `detail`.source_type = `base`.dic_code
        left join
        (select
             `data`.order_detail_id
             , `data`.activity_id
             , `data`.activity_rule_id
         from
             shyl_ods.ods_order_detail_activity_inc
         where
             dt = ${dt}
             and `type` in ( 'bootstrap-insert' , 'insert' )
             and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) `activity`
        on `detail`.id = `activity`.order_detail_id
        left join
        (select
             `data`.order_detail_id
             , `data`.coupon_id
         from
             shyl_ods.ods_order_detail_coupon_inc
         where
             dt = ${dt}
             and `type` in ( 'bootstrap-insert' , 'insert' )
             and dt in ( ${dt} , date_sub( ${dt} , 1 ) )) coupon
        on `detail`.id = `coupon`.order_detail_id
        join
        (select
             order_id
             , payment_type  as pay_type_code
             , dic_name      as pay_type_name
             , callback_time as pay_success_time
         from
             (select
                  `data`.order_id
                  , `data`.payment_type
                  , `data`.callback_time
              from
                  shyl_ods.ods_payment_info_inc
              where
                  dt = ${dt}
                  and `type` = 'update'
                  and array_contains( map_keys( `old` ) , 'payment_status' )
                  and `data`.payment_status = '1602') payment
                 left join (select
                                dic_code
                                , dic_name
                            from
                                shyl_ods.ods_base_dic_full
                            where
                                dt = ${dt}
                                and parent_code = '11') base
                           on payment.payment_type = base.dic_code) pay
        on `detail`.order_id = pay.order_id;


-- 用户退单表
drop table if exists shyl_dwd.dwd_trade_order_refund_di;
create table if not exists shyl_dwd.dwd_trade_order_refund_di
(
    order_id                string comment '订单编号' ,
    user_id                 string comment '用户id' ,
    province_id             string comment '地区id' ,
    sku_id                  string comment 'sku——id' ,
    refund_type_code        string comment '退单类型编码' ,
    refund_type_name        string comment '退单类型' ,
    refund_num              string comment '退单货数量（sku）' ,
    refund_amount           string comment '退款金额' ,
    refund_reason_type_code string comment '退单原因类型' ,
    refund_reason_type_name string comment '退单原因' ,
    refund_reason_txt       string comment '原因内容' ,
    refund_status           string comment '退款状态（0：待审批，1：已退款）' ,
    refund_time             string comment '退单时间'
) PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dwd.dwd_trade_order_refund_di partition (dt)
select
    order_id
    , user_id
    , province_id
    , sku_id
    , refund_type        as refund_type_code
    , dic1.dic_name      as refund_type_name
    , refund_num
    , refund_amount
    , refund_reason_type as refund_reason_type_code
    , dic2.dic_name      as refund_reason_type_name
    , refund_reason_txt
    , refund_status
    , refund_time
    , ${dt}
from
    (select
         `data`.order_id
         , `data`.user_id
         , `data`.sku_id
         , `data`.refund_type
         , `data`.refund_num
         , `data`.refund_amount
         , `data`.refund_reason_type
         , `data`.refund_reason_txt
         , `data`.refund_status
         , `data`.create_time as refund_time

     from
         shyl_ods.ods_order_refund_info_inc
     where
         dt = ${dt}
         and `type` in ('insert') -- 第一天没有退单的数据
    ) refund
        left join(select
                      `data`.id
                      , `data`.province_id
                  from
                      shyl_ods.ods_order_info_inc
                  where
                      dt = ${dt}
                      and `type` = 'update'
                      and `data`.order_status = '1005'
                      and array_contains( map_keys( `old` ) , 'order_status' )) info
                 on refund.order_id = info.id
        left join(select
                      dic_code
                      , dic_name
                  from
                      shyl_ods.ods_base_dic_full
                  where
                      dt = ${dt}
                      and parent_code = '15') dic1
                 on refund.refund_type = dic1.dic_code
        left join(select
                      dic_code
                      , dic_name
                  from
                      shyl_ods.ods_base_dic_full
                  where
                      dt = ${dt}
                      and parent_code = '13') dic2
                 on refund.refund_reason_type = dic2.dic_code;

--退单成功表
DROP TABLE IF EXISTS shyl_dwd.dwd_trade_refund_pay_success_di;
CREATE TABLE IF NOT EXISTS shyl_dwd.dwd_trade_refund_pay_success_di
(
    `id`                STRING COMMENT '编号' ,
    `user_id`           STRING COMMENT '用户ID' ,
    `order_id`          STRING COMMENT '订单编号' ,
    `sku_id`            STRING COMMENT 'SKU编号' ,
    `province_id`       STRING COMMENT '地区ID' ,
    `payment_type_code` STRING COMMENT '支付类型编码' ,
    `payment_type_name` STRING COMMENT '支付类型名称' ,
    `callback_time`     STRING COMMENT '支付成功时间' ,
    `refund_num`        DECIMAL(16 , 2) COMMENT '退款件数' ,
    `refund_amount`     DECIMAL(16 , 2) COMMENT '退款金额'
) COMMENT '交易域提交退款成功事务事实表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

-- 统一没有数据 只建表不导数
select
    rp.id
    , user_id
    , rp.order_id
    , rp.sku_id
    , province_id
    , payment_type
    , dic_name
    , callback_time
    , refund_num
    , total_amount
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.payment_type
         , `data`.callback_time
         , `data`.total_amount
     from
         shyl_ods.ods_refund_payment_inc
     where
         dt = ${dt}
         and `type` = 'bootstrap-insert'
         and `data`.refund_status = '1602') rp -- 模拟数据有问题，数据库的值为0702
        left join
        (select
             `data`.id
             , `data`.user_id
             , `data`.province_id
         from
             shyl_ods.ods_order_info_inc
         where
             dt = ${dt}
             and `type` = 'bootstrap-insert') oi
        on rp.order_id = oi.id
        left join
        (select
             `data`.order_id
             , `data`.sku_id
             , `data`.refund_num
         from
             shyl_ods.ods_order_refund_info_inc
         where
             dt = ${dt}
             and `type` = 'bootstrap-insert') ri
        on rp.order_id = ri.order_id
            and rp.sku_id = ri.sku_id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '11') dic
        on rp.payment_type = dic.dic_code;

-- 每日
insert overwrite table dwd_trade_refund_pay_success_di partition (dt)
select
    rp.id
    , user_id
    , rp.order_id
    , rp.sku_id
    , province_id
    , payment_type
    , dic_name
    , date_format( callback_time , 'yyyy-MM-dd' ) date_id
    , callback_time
    , refund_num
    , total_amount
from
    (select
         `data`.id
         , `data`.order_id
         , `data`.sku_id
         , `data`.payment_type
         , `data`.callback_time
         , `data`.total_amount
     from
         shyl_ods.ods_refund_payment_inc
     where
         dt = ${dt}
         and `type` = 'update'
         and array_contains( map_keys( `old` ) , 'refund_status' )
         and `data`.refund_status = '1602') rp -- 模拟数据有问题，数据库的值为0702
        left join
        (select
             `data`.id
             , `data`.user_id
             , `data`.province_id
         from
             shyl_ods.ods_order_info_inc
         where
             dt = ${dt}
             and `type` = 'update'
             and `data`.order_status = '1006'
             and array_contains( map_keys( `old` ) , 'order_status' )) oi
        on rp.order_id = oi.id
        left join
        (select
             `data`.order_id
             , `data`.sku_id
             , `data`.refund_num
         from
             shyl_ods.ods_order_refund_info_inc
         where
             dt = ${dt}
             and `type` = 'update'
             and `data`.refund_status = '0705'
             and array_contains( map_keys( `old` ) , 'refund_status' )) ri
        on rp.order_id = ri.order_id
            and rp.sku_id = ri.sku_id
        left join
        (select
             dic_code
             , dic_name
         from
             shyl_ods.ods_base_dic_full
         where
             dt = ${dt}
             and parent_code = '11') dic
        on rp.payment_type = dic.dic_code;

-- 购物车快照表
drop table if exists shyl_dwd.dwd_trade_cart_df;
create table if not exists shyl_dwd.dwd_trade_cart_df
(
    `id`     string comment '购物车id' ,
    user_id  string comment '用户id' ,
    sku_id   string comment 'sku_id' ,
    sku_name string comment 'sku名称' ,
    sku_num  int comment 'sku数量'
) COMMENT '交易域购物车周期快照表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dwd.dwd_trade_cart_df partition (dt)
select
    id
    , user_id
    , sku_id
    , sku_name
    , sku_num
    , dt
from
    shyl_ods.ods_cart_info_full
where
    dt = ${dt}
    and is_ordered = '0';

-- 活动域优惠券领取表
drop table if exists shyl_dwd.dwd_activity_coupon_get_di;
create table if not exists shyl_dwd.dwd_activity_coupon_get_di
(
    `id`        string comment 'id' ,
    `user_id`   string comment '用户id' ,
    `order_id`  string comment 'sku_id' ,
    `coupon_id` string comment '优惠券id' ,
    `get_time`  string comment '获取时间'
) COMMENT '活动域优惠券领取事务事实表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dwd.dwd_activity_coupon_get_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.order_id
    , `data`.coupon_id
    , `data`.get_time
    , date_format( `data`.get_time , 'yyyy-MM-dd' )
from
    shyl_ods.ods_coupon_use_inc
where
    `type` in ( 'bootstrap-insert' , 'insert' )
    and dt = ${dt};

drop table if exists shyl_dwd.dwd_activity_coupon_using_di;
create table if not exists shyl_dwd.dwd_activity_coupon_using_di
(
    `id`         string comment 'id' ,
    `user_id`    string comment '用户id' ,
    `order_id`   string comment 'sku_id' ,
    `coupon_id`  string comment '优惠券id' ,
    `using_time` string comment '下单时间'
) COMMENT '活动域优惠券下单事务事实表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

-- 首日
insert overwrite table shyl_dwd.dwd_activity_coupon_using_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.order_id
    , `data`.coupon_id
    , `data`.using_time
    , date_format( `data`.using_time , 'yyyy-MM-dd' )
from
    shyl_ods.ods_coupon_use_inc
where
    `type` = 'bootstrap-insert'
    and dt = ${dt}
    and `data`.using_time is not null;

insert overwrite table shyl_dwd.dwd_activity_coupon_using_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.order_id
    , `data`.coupon_id
    , `data`.using_time
    , date_format( `data`.using_time , 'yyyy-MM-dd' )
from
    shyl_ods.ods_coupon_use_inc
where
    `type` = 'update'
    and array_contains( map_keys( `old` ) , 'using_time' )
    and dt = ${dt}
    and `data`.using_time is not null;

drop table if exists shyl_dwd.dwd_activity_coupon_used_di;
create table if not exists shyl_dwd.dwd_activity_coupon_used_di
(
    `id`        string comment 'id' ,
    `user_id`   string comment '用户id' ,
    `order_id`  string comment 'sku_id' ,
    `coupon_id` string comment '优惠券id' ,
    `used_time` string comment '支付成功时间'
) COMMENT '活动域优惠券支付成功事务事实表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

-- 首日
insert overwrite table shyl_dwd.dwd_activity_coupon_used_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.order_id
    , `data`.coupon_id
    , `data`.used_time
    , date_format( `data`.used_time , 'yyyy-MM-dd' )
from
    shyl_ods.ods_coupon_use_inc
where
    `type` = 'bootstrap-insert'
    and dt = ${dt}
    and `data`.used_time is not null;

insert overwrite table shyl_dwd.dwd_activity_coupon_used_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.order_id
    , `data`.coupon_id
    , `data`.used_time
    , date_format( `data`.used_time , 'yyyy-MM-dd' )
from
    shyl_ods.ods_coupon_use_inc
where
    `type` = 'update'
    and array_contains( map_keys( `old` ) , 'used_time' )
    and dt = ${dt}
    and `data`.used_time is not null;

-- 互动域
drop table if exists shyl_dwd.dwd_interact_favor_di;
create table if not exists shyl_dwd.dwd_interact_favor_di
(
    id         string comment '收藏id' ,
    user_id    string comment '用户id' ,
    sku_id     string comment 'sku_id' ,
    spu_id     string comment 'spu——id' ,
    favor_time string comment '收藏时间'
) COMMENT '互动域收藏事务事实表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dwd.dwd_interact_favor_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.sku_id
    , `data`.spu_id
    , `data`.create_time
    , ${dt}
from
    shyl_ods.ods_favor_info_inc
where
    `type` in ( 'bootstrap-insert' , 'insert' )
    and dt = ${dt};

drop table if exists shyl_dwd.dwd_interact_comment_di;
create table if not exists shyl_dwd.dwd_interact_comment_di
(
    id            string comment '评价编号' ,
    user_id       string comment '用户id' ,
    sku_id        string comment 'sku_id' ,
    order_id      string comment '订单编号' ,
    appraise_code string comment '评价编码' ,
    appraise_name string comment '评价名称' ,
    comment_txt   string comment '评价内容' ,
    create_time   string comment '创建时间'
) COMMENT '互动域评论事务事实表'
    PARTITIONED BY (`dt` string comment '分区字段 YYYY-MM-dd')
    STORED AS ORC;

insert overwrite table shyl_dwd.dwd_interact_comment_di partition (dt)
select
    `data`.id
    , `data`.user_id
    , `data`.sku_id
    , `data`.order_id
    , `data`.appraise as appraise_code
    , base.dic_name   as appraise_name
    , `data`.comment_txt
    , `data`.create_time
    , date_format( `data`.create_time , 'yyyy-MM-dd' )
from
    shyl_ods.ods_comment_info_inc `comment`
        left join shyl_ods.ods_base_dic_full base
                  on base.dic_code = `comment`.`data`.appraise
where
    `type` in ( 'bootstrap-insert' , 'insert' )
    and base.parent_code = '12'
    and `comment`.dt = ${dt};

-- 用户域
drop table if exists shyl_dwd.dwd_user_register_di;
create table if not exists shyl_dwd.dwd_user_register_di
(
    user_id       string comment '用户id' ,
    register_time string comment '注册时间' ,
    mid_id        string comment '设备号' ,
    brand         string comment '手机品牌' ,
    phone_model   string comment '手机型号' ,
    version_code  string comment 'app版本号' ,
    os            string comment '操作系统' ,
    channel       string comment '渠道' ,
    area_code     string comment '地区编码' ,
    area_name     string comment '地区名称'
) comment '用户域用户注册事务事实表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

insert overwrite table shyl_dwd.dwd_user_register_di partition (dt)
select
    user_id
    , register_time
    , mid_id
    , brand
    , phone_model
    , version_code
    , os
    , channel
    , `hive`.area_code
    , name as area_name
    , dt
from
    (select
         `data`.`id`          as user_id
         , `data`.create_time as register_time
         , `dt`
     from
         shyl_ods.ods_user_info_inc
     where
         `type` in ( 'bootstrap-insert' , 'insert' )
--          and dt = ${dt}
    ) `mysql`
        left join(select
                      `common`.mid  as mid_id
                      , `common`.ba as brand
                      , `common`.md as phone_model
                      , `common`.vc as version_code
                      , `common`.uid
                      , `common`.os
                      , `common`.ch as channel
                      , `common`.ar as area_code
                  from
                      shyl_ods.ods_applog_par
                  where
                      `page`.page_id = 'register'
--                       and dt = ${dt}
                      and `common`.uid is not null) `hive`
                 on `mysql`.user_id = `hive`.uid
        left join
        (select
             area_code
             , `name`
         from
             shyl_ods.ods_base_province_full
         where
             dt = ${dt}) province
        on `hive`.area_code = province.area_code;


drop table if exists shyl_dwd.dwd_user_login_di;
create table if not exists shyl_dwd.dwd_user_login_di
(
    user_id      string comment '用户id' ,
    login_time   string comment '登录时间' ,
    mid_id       string comment '设备号' ,
    brand        string comment '手机品牌' ,
    phone_model  string comment '手机型号' ,
    version_code string comment 'app版本号' ,
    os           string comment '操作系统' ,
    channel      string comment '渠道' ,
    area_code    string comment '地区编码' ,
    area_name    string comment '地区名称' ,
    is_new       string comment '是否首日使用' ,
    ts           string comment '登录时间戳' ,
    session_id   string comment '会话id:设备id+时间戳'
) comment '用户域用户登录事务事实表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

-- 口径：一次会话中，到过登录页，且下一个页面uid有值，就认为登录成功
insert overwrite table shyl_dwd.dwd_user_login_di partition (dt)
select
    uid
    , from_unixtime( cast( ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as login_time
    , mid_id
    , brand
    , phone_model
    , version_code
    , os
    , channel
    , province.area_code
    , province.name                                                                   as area_name
    , is_new
    , ts
    , session_id
    , c.dt
from
    (select
         mid_id
         , brand
         , phone_model
         , version_code
         , uid
         , os
         , channel
         , area_code
         , is_new
         , page_id
         , page_flag
         , ts
         , session_id
         , lag( uid ) over (partition by session_id order by ts ) as return_uid
         , dt
     from
         (select
              mid_id
              , brand
              , phone_model
              , version_code
              , uid
              , os
              , channel
              , area_code
              , is_new
              , page_id
              , page_flag
              , ts
              , concat( mid_id , '-' ,
                        last_value( a.page_flag , true ) over (partition by a.mid_id order by ts) ) as session_id
              , dt
          from
              (select
                   `common`.mid                                        as mid_id
                   , `common`.ba                                       as brand
                   , `common`.md                                       as phone_model
                   , `common`.vc                                       as version_code
                   , `common`.uid
                   , `common`.os
                   , `common`.ch                                       as channel
                   , `common`.ar                                       as area_code
                   , `common`.is_new
                   , `page`.page_id
                   , `if`( `page`.last_page_id is null , `ts` , null ) as page_flag
                   , `ts`
                   , dt
               from
                   shyl_ods.ods_applog_par
               where
                   `page`.page_id is not null
--                    and `dt` = ${dt}
              ) a) b) c
        left join shyl_ods.ods_base_province_full province
                  on c.area_code = province.area_code
where
    province.dt = ${dt}
    and return_uid is not null
    and page_id = 'login';

select
    count( * )
    , dt
from
    shyl_dwd.dwd_user_login_di
group by
    dt;



create database shyl_dws;
/**
  dws层设计
  主题域：最近1d，nd，td；
  ??：dws=dwd+dim
  交易域：下单+退单+。。。可以放到同一张宽表中，但由于模拟环境计算资源有限，根据业务过程拆分了几张
 */
--  需求：商品粒度订单最近1日汇总表
drop table if exists shyl_dws.dws_trade_order_wide_di;
create table if not exists shyl_dws.dws_trade_order_wide_di
(
    order_id           string comment '订单id' ,
    user_id            string comment '用户id' ,
    sku_id             string comment 'sku-id' ,
    sku_name           string comment 'sku名称' ,
    sku_num            int comment 'sku数量' ,
    split_total_amount decimal(16 , 2) comment '总金额' ,
    gender             string comment '性别' ,
    age                int comment '年龄' ,
    user_level         string comment '用户等级' ,
    province_id        string comment '地区id' ,
    province_name      string comment '地区名称' ,
    brand_id           string comment '品牌id' ,
    brand_name         string comment '品牌名称' ,
    category1_id       string comment '品类一id' ,
    category1_name     string comment '品类一名称' ,
    category2_id       string comment '品类二id' ,
    category2_name     string comment '品类二名称' ,
    category3_id       string comment '品类三id' ,
    category3_name     string comment '品类三名称'
) comment '交易域下单综合统计宽表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

insert overwrite table shyl_dws.dws_trade_order_wide_di partition (dt)
select
    order_id
    , a.user_id
    , a.sku_id
    , sku_name
    , sku_num
    , split_total_amount
    , gender
    , age
    , user_level
    , province_id
    , province_name
    , brand_id
    , brand_name
    , category1_id
    , category1_name
    , category2_id
    , category2_name
    , category3_id
    , category3_name
    , a.dt
from
    (select
         order_id
         , user_id
         , sku_id
         , sku_name
         , sum( sku_num )            as sku_num
--          , sum( split_original_amount )          as split_original_amount
--          , sum( split_activity_amount )          as split_activity_amount
--          , sum( nvl( split_coupon_amount , 0 ) ) as split_coupon_amount
         , sum( split_total_amount ) as split_total_amount
         , province_id
         , dt
     from
         shyl_dwd.dwd_trade_order_di
     where
         dt = ${dt}
     group by
         order_id
         , user_id
         , sku_id
         , sku_name
         , province_id
         , dt) a
        -- 商品维度表
        left join(select
                      id
                      , sku_id
                      , brand_id
                      , brand_name
                      , category1_id
                      , category1_name
                      , category2_id
                      , category2_name
                      , category3_id
                      , category3_name
                      , dt
                  from
                      shyl_dim.dim_product_info_orc
                  where
                      dt = ${dt}) b
                 on a.sku_id = b.sku_id
        -- 地区表：地区维表只有18号分区数据
        left join (select
                       id
                       , province_name
                   from
                       shyl_dim.dim_area_info_orc
                   where
                       dt = '2023-08-18') c
                  on a.province_id = c.id
        -- 用户维度表
        left join (select
                       user_id
                       , gender
                       , floor( datediff( `current_date`( ) , birthday ) / 365 ) as age
                       , user_level
                   from
                       shyl_dim.dim_user_info_orc
                   where
                       dt = ${dt}) d
                  on a.user_id = d.user_id;

-- 数据质量量验证（去重检查：根据主键字段）
select
    count( * )
from
    (select
         1
     from
         shyl_dws.dws_trade_order_wide_di
     group by
         order_id, user_id, sku_id) a
union all
select
    count( * )
from
    shyl_dws.dws_trade_order_wide_di;
-- | \_c0 |
-- | 393 |
-- | 393 |

-- 交易域退单综合统计
drop table if exists shyl_dws.dws_trade_order_refund_wide_di;
create table if not exists shyl_dws.dws_trade_order_refund_wide_di
(
    order_id                string comment '订单id' ,
    user_id                 string comment '用户id' ,
    sku_id                  string comment 'sku-id' ,
    province_id             string comment '省份id' ,
    province_name           string comment '省份名称' ,
    gender                  string comment '性别' ,
    age                     int comment '年龄' ,
    user_level              string comment '用户等级' ,
    refund_type_code        string comment '退单类型' ,
    refund_type_name        string comment '退单类型名称' ,
    refund_num              int comment '退单货数量（sku）' ,
    refund_amount           decimal(16 , 2) comment '退单价值金额' ,
    refund_reason_type_code string comment '退单原因类型' ,
    refund_reason_type_name string comment '退单原因类型名称' ,
    brand_id                string comment '品牌id' ,
    brand_name              string comment '品牌名称' ,
    category1_id            string comment '品类一id' ,
    category1_name          string comment '品类一名称' ,
    category2_id            string comment '品类二id' ,
    category2_name          string comment '品类二名称' ,
    category3_id            string comment '品类三id' ,
    category3_name          string comment '品类三名称'
) comment '交易域退单综合统计宽表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

insert overwrite table shyl_dws.dws_trade_order_refund_wide_di partition (dt)
select
    order_id
    , a.user_id
    , a.sku_id
    , province_id
    , province_name
    , gender
    , age
    , user_level
    , refund_type_code
    , refund_type_name
    , refund_num
    , refund_amount
    , refund_reason_type_code
    , refund_reason_type_name
    , brand_id
    , brand_name
    , category1_id
    , category1_name
    , category2_id
    , category2_name
    , category3_id
    , category3_name
    , a.dt
from
    (select
         order_id
         , user_id
         , province_id
         , sku_id
         , refund_type_code
         , refund_type_name
         , sum( refund_num )    as refund_num
         , sum( refund_amount ) as refund_amount
         , refund_reason_type_code
         , refund_reason_type_name
         , dt
     from
         shyl_dwd.dwd_trade_order_refund_di
     where
         dt = ${dt}
     group by
         order_id
         , user_id
         , province_id
         , sku_id
         , refund_type_code
         , refund_type_name
         , refund_reason_type_code
         , refund_reason_type_name
         , dt) a
        -- 商品维度表
        left join(select
                      id
                      , sku_id
                      , brand_id
                      , brand_name
                      , category1_id
                      , category1_name
                      , category2_id
                      , category2_name
                      , category3_id
                      , category3_name
                      , dt
                  from
                      shyl_dim.dim_product_info_orc
                  where
                      dt = ${dt}) b
                 on a.sku_id = b.sku_id
        -- 地区表：地区维表只有18号分区数据
        left join (select
                       id
                       , province_name
                   from
                       shyl_dim.dim_area_info_orc
                   where
                       dt = '2023-08-18') c
                  on a.province_id = c.id
        -- 用户维度表
        left join (select
                       user_id
                       , gender
                       , floor( datediff( `current_date`( ) , birthday ) / 365 ) as age
                       , user_level
                   from
                       shyl_dim.dim_user_info_orc
                   where
                       dt = ${dt}) d
                  on a.user_id = d.user_id;

-- 复购：两个口径：一是根据订单的路径来判断 /数据不准确，现实场景中，复购的人一般也不走评价--》下单的页面   【数据缺失】
--              二是根据今天下单的sku_id,user_id与历史订单匹配（匹配量会随时间越来越大） 不建议 【根据业务确认，限定时间范围可以接受】
select
    a.sku_id
    , a.order_id
    , a.user_id
    , b.user_id
    , b.order_id
    , b.sku_id
    , a.dt
    , b.dt
from
    (select *
     from
         shyl_dws.dws_trade_order_wide_di
     where
         dt = ${dt}) a

        join (select *
                   from
                       shyl_dws.dws_trade_order_wide_di
                   where
                       dt < date_sub( ${dt} , 1 )) b
                  on a.sku_id = b.sku_id and a.user_id = b.user_id



