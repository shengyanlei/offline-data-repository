-- date:2023-08-15
-- author:@shyl
-- 写在前面：
/**
  ods层设计：外部表：保证数据的安全，不会因误删表而丢失数据
**/
-- 正常应该是这样设计,【由于json序列化对于元数据有影响 ，不显示中文且datagrip无法理】（猜测），于是创建一个ods_applog_par_schema 来显示表结构
drop table if exists shyl_ods.ods_applog_par;
create external table shyl_ods.ods_applog_par
(
    common   struct<ar :string,uid :string,os :string,ch :string,is_new :string,md :string,mid :string,vc :string,ba
                    :string> comment '公共信息' ,
    page     struct<page_id :string,item :string,during_time :string,last_page_id :string,source_type
                    :string> comment '页面信息' ,
    displays array<struct<page_id :string,item :string,item_type :string,pos_id :string,`order`
                          :string>> comment '动作信息' ,
    actions  array<struct<item:string,action_id:string,item_type:string,ts:bigint>> comment '曝光信息' ,
    `start`  STRUCT<entry :STRING,loading_time :BIGINT,open_ad_id :BIGINT,open_ad_ms :BIGINT,open_ad_skip_ms
                    :BIGINT> COMMENT '启动信息' ,
    `err`    STRUCT<error_code:BIGINT,msg:STRING> COMMENT '错误信息' ,
    ts       bigint comment '时间戳'
)comment '日志数据表'
    partitioned by (dt string comment '分区字段YYYY-MM-dd')
    ROW FORMAT SERDE  'org.apache.hadoop.hive.serde2.JsonSerDe'
    location '/origin_data/gmall/log/topic_log';
msck repair table shyl_ods.ods_applog_par;
show partitions shyl_ods.ods_applog_par;

--表结构表
drop table if exists shyl_ods.ods_applog_par_schema;
create table shyl_ods.ods_applog_par_schema
(
    common   struct<ar :string,uid :string,os :string,ch :string,is_new :string,md :string,mid :string,vc :string,ba
                    :string> comment '公共信息' ,
    page     struct<page_id :string,item :string,during_time :string,last_page_id :string,source_type
                    :string> comment '页面信息' ,
    displays array<struct<page_id :string,item :string,item_type :string,pos_id :string,`order`
                          :string>> comment '动作信息' ,
    actions  array<struct<item:string,action_id:string,item_type:string,ts:bigint>> comment '曝光信息' ,
    `start`  STRUCT<entry :STRING,loading_time :BIGINT,open_ad_id :BIGINT,open_ad_ms :BIGINT,open_ad_skip_ms
                    :BIGINT> COMMENT '启动信息' ,
    `err`    STRUCT<error_code:BIGINT,msg:STRING> COMMENT '错误信息' ,
    ts       bigint comment '时间戳'
)
comment'日志数据表-表结构'
    partitioned by (dt string comment '分区字段YYYY-MM-dd')
    stored as orc
    location '/origin_data/gmall/log/topic_log';
msck repair table shyl_ods.ods_applog_par_schema;
show partitions shyl_ods.ods_applog_par_schema;

/**
dwd层设计 （按天分区）
  启动日志表：公共信息 + ts + 启动信息；
  页面日志表：公共信息 + ts + 页面信息；
  动作日志表：公共信息 + ts + 动作信息 + 页面信息；
  曝光日志表：公共信息 + ts + 曝光信息 + 页面信息
  错误日志表：公共信息 + ts + null+ 页面信息 + 动作信息 + 曝光信息 + 错误信息
            union all
            公共信息 + ts + 启动信息 + null+ null + null+ 错误信息
数据示例：
公共信息：
"{""ar"":""440000"",""uid"":""568"",""os"":""iOS 13.3.1"",""ch"":""Appstore"",""is_new"":""0"",""md"":""iPhone Xs Max"",""mid"":""mid_760749"",""vc"":""v2.1.134"",""ba"":""iPhone""}",
页面信息：
"{""page_id"":""good_detail"",""item"":""26"",""during_time"":""18847"",""last_page_id"":""good_list"",""source_type"":""query""}",
曝光信息
"[{""page_id"":null,""item"":""13"",""item_type"":""sku_id"",""pos_id"":""4"",""order"":""1""},
  {""page_id"":null,""item"":""15"",""item_type"":""sku_id"",""pos_id"":""3"",""order"":""2""},
  {""page_id"":null,""item"":""12"",""item_type"":""sku_id"",""pos_id"":""4"",""order"":""3""},
  {""page_id"":null,""item"":""4"",""item_type"":""sku_id"",""pos_id"":""3"",""order"":""4""},
  {""page_id"":null,""item"":""21"",""item_type"":""sku_id"",""pos_id"":""2"",""order"":""5""}]",
动作信息：
"[{""item"":""1"",""action_id"":""get_coupon"",""item_type"":""coupon_id"",""ts"":1691044701423}]"
启动信息：
"{""entry"":""icon"",""loading_time"":5670,""open_ad_id"":16,""open_ad_ms"":3517,""open_ad_skip_ms"":0}",
报错信息
"{""error_code"":1878,""msg"":"" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)""}"

 */
--  数据探查
select *
from
    shyl_ods.ods_applog_par
where
    common is not null
    and page is not null
    and size( displays ) != -1
    and size( actions ) != -1
    and size( `array`( `start` ) ) != 0
    and err is not null
limit 1;

select *
from
    shyl_ods.ods_applog_par
where
    common is not null
    and `start`.entry is not null
    and err.error_code is not null
limit 1;

-- 创建启动日志表
-- "{""ar"":""440000"",""uid"":""568"",""os"":""iOS 13.3.1"",""ch"":""Appstore"",""is_new"":""0"",
-- ""md"":""iPhone Xs Max"",""mid"":""mid_760749"",""vc"":""v2.1.134"",""ba"":""iPhone""}",
-- "{""entry"":""icon"",""loading_time"":5670,""open_ad_id"":16,""open_ad_ms"":3517,""open_ad_skip_ms"":0}",
drop table if exists shyl_dwd.dwd_applog_start_par;
create table if not exists shyl_dwd.dwd_applog_start_par
(
    ar              string comment '地区编码' ,
    uid             string comment '会员id' ,
    os              string comment '操作系统' ,
    ch              string comment '渠道' ,
    is_new          string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    md              string comment '手机型号' ,
    mid             string comment '设备型号' ,
    vc              string comment 'app版本号' ,
    ba              string comment '手机品牌' ,
    entry           string comment '启动标识--icon手机图标 ,notice 通知 ,install 安装后启动' ,
    loading_time    string comment '启动加载时间' ,
    open_ad_id      string comment '广告页ID' ,
    open_ad_ms      string comment ' 广告总共播放时间' ,
    open_ad_skip_ms string comment '用户跳过广告时点' ,
    ts              bigint comment '时间戳'
)
    comment '启动日志表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

-- 表示启用动态分区功能。
set hive.exec.dynamic.partition=true;
-- 使用非严格模式，允许动态分区。
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table shyl_dwd.dwd_applog_start_par partition (dt)
select
    ar
    , uid
    , os
    , ch
    , is_new
    , md
    , mid
    , vc
    , ba
    , entry
    , loading_time
    , open_ad_id
    , open_ad_ms
    , open_ad_skip_ms
    , ts
    , dt
from
    shyl_ods.ods_applog_par
        lateral view inline( `array`( common ) ) t1 as ar , uid , os , ch , is_new , md , mid , vc , ba
        lateral view inline( `array`( `start` ) ) t1 as entry , loading_time , open_ad_id , open_ad_ms , open_ad_skip_ms
where
    `start`.entry is not null;

-- 查看各个分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_applog_start_par
group by
    dt;

-- 400,2023-08-18


-- 页面日志表
-- "{""ar"":""440000"",""uid"":""568"",""os"":""iOS 13.3.1"",""ch"":""Appstore"",""is_new"":""0"",""md"":""iPhone Xs Max"",
-- ""mid"":""mid_760749"",""vc"":""v2.1.134"",""ba"":""iPhone""}",
-- "{""page_id"":""good_detail"",""item"":""26"",""during_time"":""18847"",""last_page_id"":""good_list"",""source_type"":""query""}",
drop table if exists shyl_dwd.dwd_applog_page_par;
create table if not exists shyl_dwd.dwd_applog_page_par
(
    ar                string comment '地区编码' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    ch                string comment '渠道' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    md                string comment '手机型号' ,
    mid               string comment '设备型号' ,
    vc                string comment 'app版本号' ,
    ba                string comment '手机品牌' ,
    page_id           string comment '页面-页面ID' ,
    page_item         string comment '页面-目标id' ,
    page_during_time  string comment '页面-持续时间毫秒' ,
    page_last_page_id string comment '页面-上页类型' ,
    page_source_type  string comment '页面-来源类型' ,
    ts                string comment '时间戳'
) comment '页面日志表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

--插入数据
-- 表示启用动态分区功能。
set hive.exec.dynamic.partition=true;
-- 使用非严格模式，允许动态分区。
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table shyl_dwd.dwd_applog_page_par partition (dt)
select
    ar
    , uid
    , os
    , ch
    , is_new
    , md
    , mid
    , vc
    , ba
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , ts
    , dt
from
    shyl_ods.ods_applog_par
        lateral view inline( `array`( common ) ) t1 as ar , uid , os , ch , is_new , md , mid , vc , ba
        lateral view inline( `array`( page ) ) t2 as page_id , page_item , page_during_time , page_last_page_id ,
                                                     page_source_type
where
    page.page_id is not null;

-- 查看各个分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_applog_page_par
group by
    dt;

-- 2492,2023-08-18


-- 创建动作日志表
-- 数据探查
select *
from
    shyl_ods.ods_applog_par
where
    size( actions ) > 1
limit 1;
-- [{"item":"27","action_id":"cart_add","item_type":"sku_id","ts":1691044704641},
-- {"item":"1","action_id":"get_coupon","item_type":"coupon_id","ts":1691044711282}]
drop table if exists shyl_dwd.dwd_applog_actions_par;
create table if not exists shyl_dwd.dwd_applog_actions_par
(
    ar                string comment '地区编码' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    ch                string comment '渠道' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    md                string comment '手机型号' ,
    mid               string comment '设备型号' ,
    vc                string comment 'app版本号' ,
    ba                string comment '手机品牌' ,
    page_id           string comment '页面-页面ID' ,
    page_item         string comment '页面-目标id' ,
    page_during_time  string comment '页面-持续时间毫秒' ,
    page_last_page_id string comment '页面-上页类型' ,
    page_source_type  string comment '页面-来源类型' ,
    action_item       string comment '动作-目标id' ,
    action_id         string comment '动作id' ,
    action_item_type  string comment '动作-目标类型' ,
    action_ts         string comment '动作时间戳' ,
    ts                string comment '时间戳'
) comment '动作日志表'
    partitioned by (dt string comment '分区字段yyyy-mm-dd')
    stored as orc;

insert overwrite table shyl_dwd.dwd_applog_actions_par partition (dt)
select
    ar
    , uid
    , os
    , ch
    , is_new
    , md
    , mid
    , vc
    , ba
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , action.item      as action_item
    , action.action_id as action_id
    , action.item_type as action_item_type
    , action.ts        as action_ts
    , ts
    , dt
from
    shyl_ods.ods_applog_par
        lateral view inline( `array`( common ) ) t1 as ar , uid , os , ch , is_new , md , mid , vc , ba
        lateral view inline( `array`( page ) ) t2 as page_id , page_item , page_during_time , page_last_page_id ,
                                                     page_source_type
        lateral view explode( actions ) t3 as action
where
    page.page_id is not null
    and size( actions ) != -1;

--查看分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_applog_actions_par
group by
    dt;

-- 1091,2023-08-03
-- 1360,2023-08-04

-- 曝光日志表
-- "[{""page_id"":null,""item"":""13"",""item_type"":""sku_id"",""pos_id"":""4"",""order"":""1""},
--   {""page_id"":null,""item"":""15"",""item_type"":""sku_id"",""pos_id"":""3"",""order"":""2""},
--   {""page_id"":null,""item"":""12"",""item_type"":""sku_id"",""pos_id"":""4"",""order"":""3""},
--   {""page_id"":null,""item"":""4"",""item_type"":""sku_id"",""pos_id"":""3"",""order"":""4""},
--   {""page_id"":null,""item"":""21"",""item_type"":""sku_id"",""pos_id"":""2"",""order"":""5""}]",
drop table if exists shyl_dwd.dwd_applog_display_par;
create table if not exists shyl_dwd.dwd_applog_display_par
(
    ar                string comment '地区编码' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    ch                string comment '渠道' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    md                string comment '手机型号' ,
    mid               string comment '设备型号' ,
    vc                string comment 'app版本号' ,
    ba                string comment '手机品牌' ,
    page_id           string comment '页面ID' ,
    page_item         string comment '页面-目标id' ,
    page_during_time  string comment '页面-持续时间毫秒' ,
    page_last_page_id string comment '页面-上页类型' ,
    page_source_type  string comment '页面-来源类型' ,
    display_page_id   string comment '曝光-页面id' ,
    display_item      string comment '曝光-目标id' ,
    display_item_type string comment '曝光-目标类型' ,
    display_pos_id    string comment '曝光-曝光位置' ,
    display_order     string comment '曝光-出现顺序' ,
    ts                string comment '时间戳'
) comment '曝光日志表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

--插入数据
insert overwrite table shyl_dwd.dwd_applog_display_par partition (dt)
select
    ar
    , uid
    , os
    , ch
    , is_new
    , md
    , mid
    , vc
    , ba
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , display.page_id   as display_page_id
    , display.item      as display_item
    , display.item_type as display_item_type
    , display.pos_id    as display_pos_id
    , display.`order`   as display_order
    , ts
    , dt
from
    shyl_ods.ods_applog_par
        lateral view inline( `array`( common ) ) t1 as ar , uid , os , ch , is_new , md , mid , vc , ba
        lateral view inline( `array`( page ) ) t2 as page_id , page_item , page_during_time , page_last_page_id ,
                                                     page_source_type
        lateral view explode( displays ) t3 as display
where
    page.page_id is not null
    and size( displays ) != -1;

--查看分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_applog_display_par
group by
    dt;

-- 693,2023-08-18


--错误日志表 ：页面日志+启动日志（两者是不同的主题，互斥）
-- "{""error_code"":1878,""msg"":"" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)""}"

drop table if exists shyl_dwd.dwd_applog_error_par;
create table if not exists shyl_dwd.dwd_applog_error_par
(
    ar                string comment '地区编码' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    ch                string comment '渠道' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    md                string comment '手机型号' ,
    mid               string comment '设备型号' ,
    vc                string comment 'app版本号' ,
    ba                string comment '手机品牌' ,
    entry             string comment '启动标识--icon手机图标 ,notice 通知 ,install 安装后启动' ,
    loading_time      string comment '启动加载时间' ,
    open_ad_id        string comment '广告页ID' ,
    open_ad_ms        string comment ' 广告总共播放时间' ,
    open_ad_skip_ms   string comment '用户跳过广告时点' ,
    page_id           string comment '页面-页面ID' ,
    page_item         string comment '页面-目标id' ,
    page_during_time  string comment '页面-持续时间毫秒' ,
    page_last_page_id string comment '页面-上页类型' ,
    page_source_type  string comment '页面-来源类型' ,
    action_item       string comment '动作-目标id' ,
    action_id         string comment '动作id' ,
    action_item_type  string comment '动作-目标类型' ,
    action_ts         string comment '动作时间戳' ,
    display_page_id   string comment '曝光-页面id' ,
    display_item      string comment '曝光-目标id' ,
    display_item_type string comment '曝光-目标类型' ,
    display_pos_id    string comment '曝光-曝光位置' ,
    display_order     string comment '曝光-出现顺序' ,
    error_code        string comment '错误日志-错误编码' ,
    error_msg         string comment '错误日志-错误信息' ,
    ts                string comment '时间戳'
) comment '错误日志表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

--插入数据
insert overwrite table shyl_dwd.dwd_applog_error_par partition (dt)
select
    ar
    , uid
    , os
    , ch
    , is_new
    , md
    , mid
    , vc
    , ba
    , null              as entry
    , null              as loading_time
    , null              as open_ad_id
    , null              as open_ad_ms
    , null              as open_ad_skip_ms
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , action.item       as action_item
    , action.action_id  as action_id
    , action.item_type  as action_item_type
    , action.ts         as action_ts
    , display.page_id   as display_page_id
    , display.item      as display_item
    , display.item_type as display_item_type
    , display.pos_id    as display_pos_id
    , display.`order`   as display_order
    , error_code
    , error_msg
    , ts
    , dt
from
    shyl_ods.ods_applog_par
        lateral view inline( `array`( common ) ) t1 as ar , uid , os , ch , is_new , md , mid , vc , ba
        lateral view inline( `array`( page ) ) t2 as page_id , page_item , page_during_time , page_last_page_id ,
                                                     page_source_type
        --         lateral view explode(`array`(displays)) t3 as display
--         lateral view explode(`array`(actions)) t4 as action
--         lateral view inline(`array`(`start`)) t3 as entry , loading_time , open_ad_id , open_ad_ms , open_ad_skip_ms
--         lateral view inline( coalesce( `array`( `start` ) , `array`(
--                 named_struct( 'entry' , '' , 'loading_time' , cast( 0 as bigint ) , 'open_ad_id' , cast( 0 as bigint ) ,
--                               'open_ad_ms' , cast( 0 as bigint ) , 'open_ad_skip_ms' ,
--                               cast( 0 as bigint ) ) ) ) ) t3 as entry , loading_time , open_ad_id , open_ad_ms , open_ad_skip_ms
        lateral view explode( coalesce( displays , `array`(
                named_struct( 'page_id' , '' , 'item' , '' , 'item_type' , '' , 'pos_id' , '' , 'order' ,
                              '' ) ) ) ) t4 as display
        lateral view explode( coalesce( actions , `array`(
                named_struct( 'item' , '' , 'action_id' , '' , 'item_type' , '' , 'ts' ,
                              cast( 0 as bigint ) ) ) ) ) t5 as action
        lateral view inline( `array`( err ) ) t6 as error_code , error_msg
where
    err.error_code is not null
    and page.page_id is not null
union all

select
    ar
    , uid
    , os
    , ch
    , is_new
    , md
    , mid
    , vc
    , ba
    , entry
    , loading_time
    , open_ad_id
    , open_ad_ms
    , null as open_ad_skip_ms
    , null as page_id
    , null as page_item
    , null as page_during_time
    , null as page_last_page_id
    , null as page_source_type
    , null as action_item
    , null as action_id
    , null as action_item_type
    , null as action_ts
    , null as display_page_id
    , null as display_item
    , null as display_item_type
    , null as display_pos_id
    , null as display_order
    , error_code
    , error_msg
    , ts
    , dt
from
    shyl_ods.ods_applog_par
        lateral view inline( `array`( common ) ) t1 as ar , uid , os , ch , is_new , md , mid , vc , ba
        lateral view inline( `array`( `start` ) ) t3 as entry , loading_time , open_ad_id , open_ad_ms , open_ad_skip_ms
        --         lateral view inline( coalesce( `array`( `start` ) , `array`(
--                 named_struct( 'entry' , '' , 'loading_time' , cast( 0 as bigint ) , 'open_ad_id' , cast( 0 as bigint ) ,
--                               'open_ad_ms' , cast( 0 as bigint ) , 'open_ad_skip_ms' ,
--                               cast( 0 as bigint ) ) ) ) ) t3 as entry , loading_time , open_ad_id , open_ad_ms , open_ad_skip_ms

        lateral view inline( `array`( err ) ) t6 as error_code , error_msg
where
    err.error_code is not null
    and `start`.entry is not null;

--查看分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_applog_error_par
group by
    dt;

-- 294,2023-08-18


/**
  dws层设计
 */












