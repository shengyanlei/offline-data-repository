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
) comment '日志数据表'
    partitioned by (dt string comment '分区字段YYYY-MM-dd')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
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
    comment '日志数据表-表结构'
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
drop table if exists shyl_dwd.dwd_traffic_applog_start_di;
create table if not exists shyl_dwd.dwd_applog_start_di
(
    mid_id          string comment '设备号' ,
    brand           string comment '手机品牌' ,
    phone_model     string comment '手机型号' ,
    version_code    string comment 'app版本号' ,
    uid             string comment '会员id' ,
    os              string comment '操作系统' ,
    channel         string comment '渠道' ,
    area_code       string comment '地区编码' ,
    area_name       string comment '地区名称' ,
    is_new          string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    entry           string comment '启动标识--icon手机图标 ,notice 通知 ,install 安装后启动' ,
    loading_time    string comment '启动加载时间' ,
    open_ad_id      string comment '广告页ID' ,
    open_ad_ms      string comment ' 广告总共播放时间' ,
    open_ad_skip_ms string comment '用户跳过广告时点' ,
    ts              bigint comment '时间戳' ,
    start_time      string comment '启动时间 yyyy-MM-dd HH:mm:ss'
)
    comment '启动日志表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

-- 表示启用动态分区功能。
set hive.exec.dynamic.partition=true;
-- 使用非严格模式，允许动态分区。
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table shyl_dwd.dwd_traffic_applog_start_di partition (dt)
select
    `common`.mid                                                                      as mid_id
    , `common`.ba                                                                     as brand
    , `common`.md                                                                     as phone_model
    , `common`.vc                                                                     as version_code
    , `common`.uid
    , `common`.os
    , `common`.ch                                                                     as channel
    , `common`.ar                                                                     as area_code
    , province.name                                                                   as area_name
    , `common`.is_new
    , `start`.entry
    , `start`.loading_time
    , `start`.open_ad_id
    , `start`.open_ad_ms
    , `start`.open_ad_skip_ms
    , ts
    , from_unixtime( cast( ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as start_time
    , applog.dt
from
    shyl_ods.ods_applog_par applog
        left join shyl_ods.ods_base_province_full province
                  on applog.`common`.ar = province.area_code
where
    `start`.entry is not null
--     and applog.dt = ${dt} -- 脚本里面需要按天计算 这里为了方便 就注释掉了
    and province.dt = ${dt};


-- 查看各个分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_traffic_applog_start_di
group by
    dt;

-- 400,2023-08-18


-- 页面日志表
-- "{""ar"":""440000"",""uid"":""568"",""os"":""iOS 13.3.1"",""ch"":""Appstore"",""is_new"":""0"",""md"":""iPhone Xs Max"",
-- ""mid"":""mid_760749"",""vc"":""v2.1.134"",""ba"":""iPhone""}",
-- "{""page_id"":""good_detail"",""item"":""26"",""during_time"":""18847"",""last_page_id"":""good_list"",""source_type"":""query""}",
drop table if exists shyl_dwd.dwd_traffic_applog_page_di;
create table if not exists shyl_dwd.dwd_applog_page_di
(
    mid_id            string comment '设备号' ,
    brand             string comment '手机品牌' ,
    phone_model       string comment '手机型号' ,
    version_code      string comment 'app版本号' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    channel           string comment '渠道' ,
    area_code         string comment '地区编码' ,
    area_name         string comment '地区名称' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    page_id           string comment '页面-页面ID' ,
    page_item         string comment '页面-目标id' ,
    page_during_time  string comment '页面-持续时间毫秒' ,
    page_last_page_id string comment '页面-上页类型' ,
    page_source_type  string comment '页面-来源类型' ,
    session_id        string comment '会话id:设备id+时间戳' ,
    ts                string comment '时间戳' ,
    page_time         string comment '页面进入时间'
) comment '页面日志表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

--插入数据
-- 表示启用动态分区功能。
set hive.exec.dynamic.partition=true;
-- 使用非严格模式，允许动态分区。
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table shyl_dwd.dwd_traffic_applog_page_di partition (dt);
select
    mid_id
    , brand
    , phone_model
    , version_code
    , uid
    , os
    , channel
    , area_code
    , area_name
    , is_new
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , concat( mid_id , '-' , last_value( session_flag , true ) over (partition by mid_id order by ts ) )
    , ts
    , from_unixtime( cast( ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as page_time
    , dt
from
    (select
         `common`.mid                                    as mid_id
         , `common`.ba                                   as brand
         , `common`.md                                   as phone_model
         , `common`.vc                                   as version_code
         , `common`.uid
         , `common`.os
         , `common`.ch                                   as channel
         , `common`.ar                                   as area_code
         , province.name                                 as area_name
         , `common`.is_new
         , `page`.page_id
         , `page`.item                                   as page_item
         , `page`.during_time                            as page_during_time
         , `page`.last_page_id                           as page_last_page_id
         , `page`.source_type                            as page_source_type
         , if( `page`.last_page_id is null , ts , null ) as session_flag
         , ts
         , applog.dt
     from
         shyl_ods.ods_applog_par applog
             left join shyl_ods.ods_base_province_full province
                       on applog.`common`.ar = province.area_code
     where
         applog.`page`.page_id is not null
--       and applog.dt = ${dt}
         and province.dt = ${dt}) a;


-- 查看各个分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_traffic_applog_page_di
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
drop table if exists shyl_dwd.dwd_traffic_applog_actions_di;
create table if not exists shyl_dwd.dwd_traffic_applog_actions_di
(
    mid_id            string comment '设备号' ,
    brand             string comment '手机品牌' ,
    phone_model       string comment '手机型号' ,
    version_code      string comment 'app版本号' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    channel           string comment '渠道' ,
    area_code         string comment '地区编码' ,
    area_name         string comment '地区名称' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    page_id           string comment '页面-页面ID' ,
    page_item         string comment '页面-目标id' ,
    page_during_time  string comment '页面-持续时间毫秒' ,
    page_last_page_id string comment '页面-上页类型' ,
    page_source_type  string comment '页面-来源类型' ,
    action_item       string comment '动作-目标id' ,
    action_id         string comment '动作id' ,
    action_item_type  string comment '动作-目标类型' ,
    action_ts         string comment '动作发生时间戳' ,
    action_time       string comment '动作发生时间' ,
    ts                string comment '日志时间戳'
) comment '流量域动作日志事实表'
    partitioned by (dt string comment '分区字段yyyy-mm-dd')
    stored as orc;

insert overwrite table shyl_dwd.dwd_traffic_applog_actions_di partition (dt)
select
    mid_id
    , brand
    , phone_model
    , version_code
    , uid
    , os
    , channel
    , action.area_code
    , province.name as area_name
    , is_new
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , action_item
    , action_id
    , action_item_type
    , action_ts
    , action_time
    , ts
    , action.dt
from
    (select
         `common`.mid                                                                             as mid_id
         , `common`.ba                                                                            as brand
         , `common`.md                                                                            as phone_model
         , `common`.vc                                                                            as version_code
         , `common`.uid
         , `common`.os
         , `common`.ch                                                                            as channel
         , `common`.ar                                                                            as area_code
         , `common`.is_new
         , `page`.page_id
         , `page`.item                                                                            as page_item
         , `page`.during_time                                                                     as page_during_time
         , `page`.last_page_id                                                                    as page_last_page_id
         , `page`.source_type                                                                     as page_source_type
         , action.item                                                                            as action_item
         , action.action_id                                                                       as action_id
         , action.item_type                                                                       as action_item_type
         , action.ts                                                                              as action_ts
         , from_unixtime( cast( action.ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as action_time
         , ts
         , dt
     from
         shyl_ods.ods_applog_par applog
             lateral view explode( actions ) t3 as action
     where
         `page`.page_id is not null
--          and applog.dt = ${dt}
         and size( actions ) != -1) action
        left join shyl_ods.ods_base_province_full province
                  on action.area_code = province.area_code
                      and province.dt = ${dt};

--查看分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_applog_actions_di
group by
    dt;


-- 曝光日志表
-- "[{""page_id"":null,""item"":""13"",""item_type"":""sku_id"",""pos_id"":""4"",""order"":""1""},
--   {""page_id"":null,""item"":""15"",""item_type"":""sku_id"",""pos_id"":""3"",""order"":""2""},
--   {""page_id"":null,""item"":""12"",""item_type"":""sku_id"",""pos_id"":""4"",""order"":""3""},
--   {""page_id"":null,""item"":""4"",""item_type"":""sku_id"",""pos_id"":""3"",""order"":""4""},
--   {""page_id"":null,""item"":""21"",""item_type"":""sku_id"",""pos_id"":""2"",""order"":""5""}]",
drop table if exists shyl_dwd.dwd_traffic_applog_display_di;
create table if not exists shyl_dwd.dwd_traffic_applog_display_di
(
    mid_id            string comment '设备号' ,
    brand             string comment '手机品牌' ,
    phone_model       string comment '手机型号' ,
    version_code      string comment 'app版本号' ,
    uid               string comment '会员id' ,
    os                string comment '操作系统' ,
    channel           string comment '渠道' ,
    area_code         string comment '地区编码' ,
    area_name         string comment '地区名称' ,
    is_new            string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
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
    ts                string comment '时间戳' ,
    display_time      string comment '曝光时间'
) comment '流量域曝光日志事务事实表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

--插入数据
insert overwrite table shyl_dwd.dwd_traffic_applog_display_di partition (dt)
select
    mid_id
    , brand
    , phone_model
    , version_code
    , uid
    , os
    , channel
    , display.area_code
    , province.name as area_name
    , is_new
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , display_page_id
    , display_item
    , display_item_type
    , display_pos_id
    , display_order
    , ts
    , display_time
    , display.dt
from
    (select
         `common`.mid                                                                      as mid_id
         , `common`.ba                                                                     as brand
         , `common`.md                                                                     as phone_model
         , `common`.vc                                                                     as version_code
         , `common`.uid
         , `common`.os
         , `common`.ch                                                                     as channel
         , `common`.ar                                                                     as area_code
         , `common`.is_new
         , `page`.page_id
         , `page`.item                                                                     as page_item
         , `page`.during_time                                                              as page_during_time
         , `page`.last_page_id                                                             as page_last_page_id
         , `page`.source_type                                                              as page_source_type
         , display.page_id                                                                 as display_page_id
         , display.item                                                                    as display_item
         , display.item_type                                                               as display_item_type
         , display.pos_id                                                                  as display_pos_id
         , display.`order`                                                                 as display_order
         , ts
         , from_unixtime( cast( ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as display_time
         , dt
     from
         shyl_ods.ods_applog_par
             lateral view explode( displays ) t3 as display
     where
         `page`.page_id is not null
         and size( displays ) != -1
--          and dt = ${dt}
    ) display
        left join shyl_ods.ods_base_province_full province
                  on display.area_code = province.area_code
                      and province.dt = ${dt};

--查看分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_traffic_applog_display_di
group by
    dt;



--错误日志表 ：页面日志+启动日志（两者是不同的主题，互斥）
-- "{""error_code"":1878,""msg"":"" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)""}"

drop table if exists shyl_dwd.dwd_traffic_applog_error_di;
create table if not exists shyl_dwd.dwd_traffic_applog_error_di
(
    mid_id                string comment '设备号' ,
    brand                 string comment '手机品牌' ,
    phone_model           string comment '手机型号' ,
    version_code          string comment 'app版本号' ,
    uid                   string comment '会员id' ,
    os                    string comment '操作系统' ,
    channel               string comment '渠道' ,
    area_code             string comment '地区编码' ,
    area_name             string comment '地区名称' ,
    is_new                string comment '是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0' ,
    start_entry           string comment '启动-启动标识--icon手机图标 ,notice 通知 ,install 安装后启动' ,
    start_loading_time    string comment '启动-启动加载时间' ,
    start_open_ad_id      string comment '启动-广告页ID' ,
    start_open_ad_ms      string comment '启动-广告总共播放时间' ,
    start_open_ad_skip_ms string comment '启动-用户跳过广告时点' ,
    page_id               string comment '页面-页面ID' ,
    page_item             string comment '页面-目标id' ,
    page_during_time      string comment '页面-持续时间毫秒' ,
    page_last_page_id     string comment '页面-上页类型' ,
    page_source_type      string comment '页面-来源类型' ,
    action_item           string comment '动作-目标id' ,
    action_id             string comment '动作id' ,
    action_item_type      string comment '动作-目标类型' ,
    action_ts             string comment '动作时间戳' ,
    display_page_id       string comment '曝光-页面id' ,
    display_item          string comment '曝光-目标id' ,
    display_item_type     string comment '曝光-目标类型' ,
    display_pos_id        string comment '曝光-曝光位置' ,
    display_order         string comment '曝光-出现顺序' ,
    error_code            string comment '错误日志-错误编码' ,
    error_msg             string comment '错误日志-错误信息' ,
    error_type            string comment '错误日志-错误类型【启动/页面】' ,
    ts                    string comment '时间戳' ,
    error_time            string comment '错误时间'
) comment '流量域错误日志事实表'
    partitioned by (dt string comment '分区字段yyyy-MM-dd')
    stored as orc;

--插入数据
insert overwrite table shyl_dwd.dwd_traffic_applog_error_di partition (dt)
select
    mid_id
    , brand
    , phone_model
    , version_code
    , uid
    , os
    , channel
    , error.area_code
     ,province.name as area_name
    , is_new
    , start_entry
    , start_loading_time
    , start_open_ad_id
    , start_open_ad_ms
    , start_open_ad_skip_ms
    , page_id
    , page_item
    , page_during_time
    , page_last_page_id
    , page_source_type
    , action_item
    , action_id
    , action_item_type
    , action_ts
    , display_page_id
    , display_item
    , display_item_type
    , display_pos_id
    , display_order
    , error_code
    , error_msg
    , error_type
    , ts
    , error_time
    , error.dt
from
    (select
         `common`.mid                                                                      as mid_id
         , `common`.ba                                                                     as brand
         , `common`.md                                                                     as phone_model
         , `common`.vc                                                                     as version_code
         , `common`.uid
         , `common`.os
         , `common`.ch                                                                     as channel
         , `common`.ar                                                                     as area_code
         , `common`.is_new
         , null                                                                            as start_entry
         , null                                                                            as start_loading_time
         , null                                                                            as start_open_ad_id
         , null                                                                            as start_open_ad_ms
         , null                                                                            as start_open_ad_skip_ms
         , `page`.page_id
         , `page`.item                                                                     as page_item
         , `page`.during_time                                                              as page_during_time
         , `page`.last_page_id                                                             as page_last_page_id
         , `page`.source_type                                                              as page_source_type
         , action.item                                                                     as action_item
         , action.action_id                                                                as action_id
         , action.item_type                                                                as action_item_type
         , action.ts                                                                       as action_ts
         , display.page_id                                                                 as display_page_id
         , display.item                                                                    as display_item
         , display.item_type                                                               as display_item_type
         , display.pos_id                                                                  as display_pos_id
         , display.`order`                                                                 as display_order
         , `err`.error_code
         , `err`.msg                                                                       as error_msg
         , 'page'                                                                          as error_type
         , ts
         , from_unixtime( cast( ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as error_time
         , dt
     from
         shyl_ods.ods_applog_par
             lateral view explode( coalesce( displays , `array`(
                     named_struct( 'page_id' , 'NULL' , 'item' , 'NULL' , 'item_type' , 'NULL' , 'pos_id' , 'NULL' ,
                                   'order' ,
                                   'NULL' ) ) ) ) t4 as display --防止 display为空丢失数据
             lateral view explode( coalesce( actions , `array`(
                     named_struct( 'item' , 'NULL' , 'action_id' , 'NULL' , 'item_type' , 'NULL' , 'ts' ,
                                   cast( 0 as bigint ) ) ) ) ) t5 as action --防止 action为空丢失数据
     where
         err.error_code is not null
         and page.page_id is not null
--     and dt = ${dt}
     union all

     select
         `common`.mid                                                                      as mid_id
         , `common`.ba                                                                     as brand
         , `common`.md                                                                     as phone_model
         , `common`.vc                                                                     as version_code
         , `common`.uid
         , `common`.os
         , `common`.ch                                                                     as channel
         , `common`.ar                                                                     as area_code
         , `common`.is_new
         , `start`.entry                                                                   as start_entry
         , `start`.loading_time                                                            as start_loading_time
         , `start`.open_ad_id                                                              as start_open_ad_id
         , `start`.open_ad_ms                                                              as start_open_ad_ms
         , `start`.open_ad_skip_ms                                                         as start_pen_ad_skip_ms
         , null                                                                            as page_id
         , null                                                                            as page_item
         , null                                                                            as page_during_time
         , null                                                                            as page_last_page_id
         , null                                                                            as page_source_type
         , null                                                                            as action_item
         , null                                                                            as action_id
         , null                                                                            as action_item_type
         , null                                                                            as action_ts
         , null                                                                            as display_page_id
         , null                                                                            as display_item
         , null                                                                            as display_item_type
         , null                                                                            as display_pos_id
         , null                                                                            as display_order
         , `err`.error_code
         , `err`.msg                                                                       as error_msg
         , 'start'                                                                         as error_type
         , ts
         , from_unixtime( cast( ts / 1000 + 8 * 60 * 60 as int ) , 'yyyy-MM-dd HH:mm:ss' ) as error_time
         , dt
     from
         shyl_ods.ods_applog_par
     where
         err.error_code is not null
--     and dt = ${dt}
         and `start`.entry is not null) `error`
        left join shyl_ods.ods_base_province_full province
                  on error.area_code = province.area_code
                      and province.dt = ${dt};

--查看分区数据量
select
    count( * )
    , dt
from
    shyl_dwd.dwd_traffic_applog_error_di
group by
    dt;
















