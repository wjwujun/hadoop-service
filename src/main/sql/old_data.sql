--原始数据，日志清理后的数据
CREATE EXTERNAL TABLE ods_app_log (
    sdk_ver string
    ,time_zone string
    ,commit_id string
    ,commit_time string
    ,pid string
    ,app_token string
    ,app_id string
    ,device_id string
    ,device_id_type string
    ,release_channel string
    ,app_ver_name string
    ,app_ver_code string
    ,os_name string
    ,os_ver string
    ,LANGUAGE string
    ,country string
    ,manufacture string
    ,device_model string
    ,resolution string
    ,net_type string
    ,account string
    ,app_device_id string
    ,mac string
    ,android_id string
    ,imei string
    ,cid_sn string
    ,build_num string
    ,mobile_data_type string
    ,promotion_channel string
    ,carrier string
    ,city string
    ,user_id string
    ) partitioned BY (
    day string
    ,os string
    ) row format delimited fields terminated BY '\001' location '/app/clean/2019-04-03';

ALTER TABLE ods_app_log ADD PARTITION (day = '2019-04-03',os = 'android') location '/app/clean/2019-04-03/android';
ALTER TABLE ods_app_log ADD PARTITION (day = '2019-04-03',os = 'ios') location '/app/clean/2019-04-03/ios';




