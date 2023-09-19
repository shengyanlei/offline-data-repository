-- date:2023-09-01
-- author:@shyl
-- 写在前面：
/**
  测试 业务数据 外部表 还是内部表好
  结论：最源头的数据其实保留在业务数据库中，可以随时拉去，所以内外都可，这里使用外部表
 */
-- 全量表 "activity_info","activity_rule","base_category1","base_category2","base_category3","base_dic","base_province","base_region"
--         ,"base_trademark","cart_info","coupon_info","coupon_info","sku_attr_value","sku_sale_attr_value","sku_info","spu_info"
drop table if exists shyl_ods.ods_activity_info_full;
CREATE external TABLE shyl_ods.ods_activity_info_full (
  id BIGINT COMMENT '活动id',
  activity_name STRING COMMENT '活动名称',
  activity_type STRING COMMENT '活动类型（1：满减，2：折扣）',
  activity_desc STRING COMMENT '活动描述',
  start_time string COMMENT '开始时间',
  end_time string COMMENT '结束时间',
  create_time string COMMENT '创建时间'
)
COMMENT '活动表'
partitioned by (dt string comment '分区字段yyyy-MM-dd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS ORC
location "/origin_data/gmall/db/activity_info_full";

msck repair table shyl_ods.ods_activity_info_full;
show partitions shyl_ods.ods_activity_info_full;
select * from shyl_ods.ods_activity_info_full limit 10;

drop table if exists shyl_ods.ods_activity_rule_full;
CREATE external TABLE if not exists shyl_ods.ods_activity_rule_full (
  id BIGINT COMMENT '编号',
  activity_id STRING COMMENT '活动id',
  activity_type STRING COMMENT '活动类型（1：满减，2：折扣）',
  condition_amount STRING COMMENT '满减金额',
  condition_num bigint COMMENT '满减件数',
  benefit_amount string COMMENT '优惠金额',
  benefit_discount string COMMENT '优惠折扣',
  benefit_level bigint comment  '优惠级别'
)
COMMENT '优惠规则表'
partitioned by (dt string comment '分区字段yyyy-MM-dd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS ORC
location "/origin_data/gmall/db/activity_rule_full";

msck repair table shyl_ods.ods_activity_rule_full;
show partitions shyl_ods.ods_activity_rule_full;
select * from shyl_ods.ods_activity_rule_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_category1_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_category1_full
(
    `id`   STRING COMMENT '编号',
    `name` STRING COMMENT '分类名称'
) COMMENT '一级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_category1_full/';

msck repair table shyl_ods.ods_base_category1_full;
show partitions shyl_ods.ods_base_category1_full;
select * from shyl_ods.ods_base_category1_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_category2_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_category2_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '二级分类名称',
    `category1_id` STRING COMMENT '一级分类编号'
) COMMENT '二级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_category2_full/';

msck repair table shyl_ods.ods_base_category2_full;
show partitions shyl_ods.ods_base_category2_full;
select * from shyl_ods.ods_base_category2_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_category3_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_category3_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号'
) COMMENT '三级品类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_category3_full/';

msck repair table shyl_ods.ods_base_category3_full;
show partitions shyl_ods.ods_base_category3_full;
select * from shyl_ods.ods_base_category3_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_dic_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_dic_full/';

msck repair table shyl_ods.ods_base_dic_full;
show partitions shyl_ods.ods_base_dic_full;
select * from shyl_ods.ods_base_dic_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_province_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_province_full
(
    `id`         STRING COMMENT '编号',
    `name`       STRING COMMENT '省份名称',
    `region_id`  STRING COMMENT '地区ID',
    `area_code`  STRING COMMENT '地区编码',
    `iso_code`   STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2` STRING COMMENT '新版IOS-3166-2编码，供可视化使用'
) COMMENT '省份表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_province_full/';

msck repair table shyl_ods.ods_base_province_full;
show partitions shyl_ods.ods_base_province_full;
select * from shyl_ods.ods_base_province_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_region_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_region_full
(
    `id`          STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_region_full/';

msck repair table shyl_ods.ods_base_region_full;
show partitions shyl_ods.ods_base_region_full;
select * from shyl_ods.ods_base_region_full ;

DROP TABLE IF EXISTS shyl_ods.ods_base_trademark_full;
CREATE EXTERNAL TABLE shyl_ods.ods_base_trademark_full
(
    `id`       STRING COMMENT '编号',
    `tm_name`  STRING COMMENT '品牌名称',
    `logo_url` STRING COMMENT '品牌logo的图片路径'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/base_trademark_full/';

msck repair table shyl_ods.ods_base_trademark_full;
show partitions shyl_ods.ods_base_trademark_full;
select * from shyl_ods.ods_base_trademark_full ;

DROP TABLE IF EXISTS shyl_ods.ods_cart_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_cart_info_full
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户id',
    `sku_id`       STRING COMMENT 'sku_id',
    `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
    `sku_num`      BIGINT COMMENT '数量',
    `img_url`      BIGINT COMMENT '商品图片地址',
    `sku_name`     STRING COMMENT 'sku名称 (冗余)',
    `is_checked`   STRING COMMENT '是否被选中',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间',
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号'
) COMMENT '购物车全量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/cart_info_full/';

msck repair table shyl_ods.ods_cart_info_full;
show partitions shyl_ods.ods_cart_info_full;
select * from shyl_ods.ods_cart_info_full ;

DROP TABLE IF EXISTS shyl_ods.ods_coupon_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_coupon_info_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始领取时间',
    `end_time`         STRING COMMENT '结束领取时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间'
) COMMENT '优惠券信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/coupon_info_full/';

msck repair table shyl_ods.ods_coupon_info_full;
show partitions shyl_ods.ods_coupon_info_full;
select * from shyl_ods.ods_coupon_info_full;

DROP TABLE IF EXISTS shyl_ods.ods_sku_attr_value_full;
CREATE EXTERNAL TABLE shyl_ods.ods_sku_attr_value_full
(
    `id`         STRING COMMENT '编号',
    `attr_id`    STRING COMMENT '平台属性ID',
    `value_id`   STRING COMMENT '平台属性值ID',
    `sku_id`     STRING COMMENT '商品ID',
    `attr_name`  STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/sku_attr_value_full/';

msck repair table shyl_ods.ods_sku_attr_value_full;
show partitions shyl_ods.ods_sku_attr_value_full;
select * from shyl_ods.ods_sku_attr_value_full;

DROP TABLE IF EXISTS shyl_ods.ods_sku_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_sku_info_full
(
    `id`              STRING COMMENT 'skuId',
    `spu_id`          STRING COMMENT 'spuid',
    `price`           DECIMAL(16, 2) COMMENT '价格',
    `sku_name`        STRING COMMENT '商品名称',
    `sku_desc`        STRING COMMENT '商品描述',
    `weight`          DECIMAL(16, 2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌id',
    `category3_id`    STRING COMMENT '品类id',
    `sku_default_igm` STRING COMMENT '商品图片地址',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/sku_info_full/';

msck repair table shyl_ods.ods_sku_info_full;
show partitions shyl_ods.ods_sku_info_full;
select * from shyl_ods.ods_sku_info_full;

DROP TABLE IF EXISTS shyl_ods.ods_sku_sale_attr_value_full;
CREATE EXTERNAL TABLE shyl_ods.ods_sku_sale_attr_value_full
(
    `id`                   STRING COMMENT '编号',
    `sku_id`               STRING COMMENT 'sku_id',
    `spu_id`               STRING COMMENT 'spu_id',
    `sale_attr_value_id`   STRING COMMENT '销售属性值id',
    `sale_attr_id`         STRING COMMENT '销售属性id',
    `sale_attr_name`       STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/sku_sale_attr_value_full/';

msck repair table shyl_ods.ods_sku_sale_attr_value_full;
show partitions shyl_ods.ods_sku_sale_attr_value_full;
select * from shyl_ods.ods_sku_sale_attr_value_full;

DROP TABLE IF EXISTS shyl_ods.ods_spu_info_full;
CREATE EXTERNAL TABLE shyl_ods.ods_spu_info_full
(
    `id`           STRING COMMENT 'spu_id',
    `spu_name`     STRING COMMENT 'spu名称',
    `description`  STRING COMMENT '描述信息',
    `category3_id` STRING COMMENT '品类id',
    `tm_id`        STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION '/origin_data/gmall/db/spu_info_full/';

msck repair table shyl_ods.ods_spu_info_full;
show partitions shyl_ods.ods_spu_info_full;
select * from shyl_ods.ods_spu_info_full;



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
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,cart_price :DECIMAL(16, 2),sku_num :BIGINT,img_url :STRING,sku_name
                  :STRING,is_checked :STRING,create_time :STRING,operate_time :STRING,is_ordered :STRING,order_time
                  :STRING,source_type :STRING,source_id :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '购物车增量表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/cart_info_inc/';

msck repair table shyl_ods.ods_cart_info_inc;
show partitions shyl_ods.ods_cart_info_inc;
select * from shyl_ods.ods_cart_info_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_comment_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_comment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,nick_name :STRING,head_img :STRING,sku_id :STRING,spu_id :STRING,order_id
                  :STRING,appraise :STRING,comment_txt :STRING,create_time :STRING,operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '评价表'

    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/comment_info_inc/';

msck repair table shyl_ods.ods_comment_info_inc;
show partitions shyl_ods.ods_comment_info_inc;
select * from shyl_ods.ods_comment_info_inc limit 10;


DROP TABLE IF EXISTS shyl_ods.ods_coupon_use_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_coupon_use_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,coupon_id :STRING,user_id :STRING,order_id :STRING,coupon_status :STRING,get_time :STRING,using_time
                  :STRING,used_time :STRING,expire_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/coupon_use_inc/';

msck repair table shyl_ods.ods_coupon_use_inc;
show partitions shyl_ods.ods_coupon_use_inc;
select * from shyl_ods.ods_coupon_use_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_favor_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_favor_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,spu_id :STRING,is_cancel :STRING,create_time :STRING,cancel_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/favor_info_inc/';

msck repair table shyl_ods.ods_favor_info_inc;
show partitions shyl_ods.ods_favor_info_inc;
select * from shyl_ods.ods_favor_info_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_detail_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_detail_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,sku_id :STRING,sku_name :STRING,img_url :STRING,order_price
                  :DECIMAL(16, 2),sku_num :BIGINT,create_time :STRING,source_type :STRING,source_id :STRING,split_total_amount
                  :DECIMAL(16, 2),split_activity_amount :DECIMAL(16, 2),split_coupon_amount
                  :DECIMAL(16, 2)> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_detail_inc/';

msck repair table shyl_ods.ods_order_detail_inc;
show partitions shyl_ods.ods_order_detail_inc;
select * from shyl_ods.ods_order_detail_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_detail_activity_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_detail_activity_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,activity_id :STRING,activity_rule_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细活动关联表'

    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_detail_activity_inc/';

msck repair table shyl_ods.ods_order_detail_activity_inc;
show partitions shyl_ods.ods_order_detail_activity_inc;
select * from shyl_ods.ods_order_detail_activity_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_detail_coupon_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_detail_coupon_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,coupon_id :STRING,coupon_use_id :STRING,sku_id
                  :STRING,create_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细优惠券关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_detail_coupon_inc/';

msck repair table shyl_ods.ods_order_detail_coupon_inc;
show partitions shyl_ods.ods_order_detail_coupon_inc;
select * from shyl_ods.ods_order_detail_coupon_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,consignee :STRING,consignee_tel :STRING,total_amount :DECIMAL(16, 2),order_status :STRING,user_id
                  :STRING,payment_way :STRING,delivery_address :STRING,order_comment :STRING,out_trade_no :STRING,trade_body
                  :STRING,create_time :STRING,operate_time :STRING,expire_time :STRING,process_status :STRING,tracking_no
                  :STRING,parent_order_id :STRING,img_url :STRING,province_id :STRING,activity_reduce_amount
                  :DECIMAL(16, 2),coupon_reduce_amount :DECIMAL(16, 2),original_total_amount :DECIMAL(16, 2),freight_fee
                  :DECIMAL(16, 2),freight_fee_reduce :DECIMAL(16, 2),refundable_time :DECIMAL(16, 2)> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_info_inc/';

msck repair table shyl_ods.ods_order_info_inc;
show partitions shyl_ods.ods_order_info_inc;
select * from shyl_ods.ods_order_info_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_refund_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_refund_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                  :DECIMAL(16, 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_refund_info_inc/';

msck repair table shyl_ods.ods_order_refund_info_inc;
show partitions shyl_ods.ods_order_refund_info_inc;
select * from shyl_ods.ods_order_refund_info_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_order_status_log_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_order_status_log_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                  :DECIMAL(16, 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单状态流水表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/order_status_log_inc/';

msck repair table shyl_ods.ods_order_status_log_inc;
show partitions shyl_ods.ods_order_status_log_inc;
select * from shyl_ods.ods_order_status_log_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_payment_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_payment_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,user_id :STRING,payment_type :STRING,trade_no
                  :STRING,total_amount :DECIMAL(16, 2),subject :STRING,payment_status :STRING,create_time :STRING,callback_time
                  :STRING,callback_content :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '支付表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/payment_info_inc/';

msck repair table shyl_ods.ods_payment_info_inc;
show partitions shyl_ods.ods_payment_info_inc;
select * from shyl_ods.ods_payment_info_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_refund_payment_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_refund_payment_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,sku_id :STRING,payment_type :STRING,trade_no :STRING,total_amount
                  :DECIMAL(16, 2),subject :STRING,refund_status :STRING,create_time :STRING,callback_time :STRING,callback_content
                  :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '退款表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/refund_payment_inc/';

msck repair table shyl_ods.ods_refund_payment_inc;
show partitions shyl_ods.ods_refund_payment_inc;
select * from shyl_ods.ods_refund_payment_inc limit 10;

DROP TABLE IF EXISTS shyl_ods.ods_user_info_inc;
CREATE EXTERNAL TABLE shyl_ods.ods_user_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,login_name :STRING,nick_name :STRING,passwd :STRING,name :STRING,phone_num :STRING,email
                  :STRING,head_img :STRING,user_level :STRING,birthday :STRING,gender :STRING,create_time :STRING,operate_time
                  :STRING,status :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/origin_data/gmall/db/user_info_inc/';

msck repair table shyl_ods.ods_user_info_inc;
show partitions shyl_ods.ods_user_info_inc;
select * from shyl_ods.ods_user_info_inc where dt ='2023-08-21';





