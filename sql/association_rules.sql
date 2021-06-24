#https://en.wikipedia.org/wiki/Association_rule_learning
# this algorithm won't work well unless you do not finish pre-processing such as reducing data complexity, extracting only valuable information.


DROP TABLE IF EXISTS sub_data.adspot_recommendation;
CREATE TABLE sub_data.adspot_recommendation AS(
  WITH USERFILTER AS(
    SELECT 
      uid
    FROM sub_data.did_uid_daily_summary --최신28일분이 들어가있음
    GROUP BY uid
    HAVING count(1)>10000  -- 1만이상 IMP가 나온 유저는 포함시키지 않기위해
  ),
  -- adspot과유저의 linked table생성 
  DID_UID AS(
    SELECT 
      did_uid_daily_summary.did, --adspot ID
      did_uid_daily_summary.uid -- 유저식별 ID
    FROM sub_data.did_uid_daily_summary
    WHERE did_uid_daily_summary.uid not in (SELECT USERFILTER.uid FROM USERFILTER)
    GROUP BY did_uid_daily_summary.did, did_uid_daily_summary.uid
    HAVING count(1)>2 --3일이상 방문한 유저만을 필터링(정상유저를 필터링)
  ),
  USER_LEVEL AS(
  SELECT 
      conversion.uid, --유저 필터링
      MAX(conversion.time) as last_cv_time,
      MAX(CASE WHEN sub_conversion.conversion_property_kind = 1 THEN 1 ELSE 0 END) AS low, -- 최초기동 conversion
      MAX(CASE WHEN sub_conversion.conversion_property_kind != 1 THEN 10 ELSE 0 END) AS middle,-- 최초기동이 아닌 conversion
      MAX(CASE WHEN LOWER(sub_conversion.name) LIKE '%buy%'
        OR LOWER(sub_conversion.name) LIKE '%purchase%'
      THEN 100 ELSE 0 END) AS high ---과금 conversion
  FROM nend_log.conversion 
  INNER JOIN nend_log.sub_conversion on(conversion.sub_conversion_property_id=sub_conversion.id)
  WHERE TD_TIME_RANGE(conversion.time, TD_TIME_ADD(TD_DATE_TRUNC('day', TD_SCHEDULED_TIME(),'JST'), '-180d'), NULL, 'JST') -- 최근 반년
  GROUP BY uid
  ),
  CV AS(
    SELECT 
      ad_camp.advertiser_promotion_id as pid, --프로모션ID
      conversion.uid, --유저식별 ID
      count(1) as cnt
    FROM nend_log.conversion
      INNER JOIN nend_master.ad_camp on conversion.advertiser_campaign_id= ad_camp.id
      INNER JOIN nend_log.sub_conversion on(conversion.sub_conversion_property_id=sub_conversion.id)
    WHERE TD_TIME_RANGE(conversion.time, TD_TIME_ADD(TD_DATE_TRUNC('day', TD_SCHEDULED_TIME(),'JST'), '-180d'), NULL, 'JST') -- 최근 반년
    AND conversion.uid not in (SELECT USERFILTER.uid FROM USERFILTER)
    GROUP BY uid, ad_camp.advertiser_promotion_id
  ),
  PID_UID AS(
  SELECT * FROM (
    SELECT 
      CV.pid, --프로모션ID
      CV.uid, --유저식별 ID
      ROW_NUMBER() OVER (PARTITION BY CV.pid ORDER BY USER_LEVEL.high + USER_LEVEL.middle + USER_LEVEL.low DESC, USER_LEVEL.last_cv_time DESC) AS num
    FROM CV INNER JOIN USER_LEVEL 
    ON CV.uid= USER_LEVEL.uid
    ORDER BY CV.pid DESC
    )
    WHERE num<50000 --최대 5만유저
  ),
  -- user gruop X
  X AS(
    SELECT 
      pid,
      count(1) as cnt
    FROM PID_UID
    GROUP BY pid
    HAVING count(1)>10 
  ),
  -- user gruop Y
  Y AS(
    SELECT 
      did, 
      count(1) as cnt 
    FROM DID_UID 
    GROUP BY did
  ),
  -- LINKED_XY
  XY AS (
    SELECT
      PID_UID.pid, 
      DID_UID.did,
      COUNT(1) AS cnt
    FROM PID_UID
      INNER JOIN DID_UID ON PID_UID.uid = DID_UID.uid
    GROUP BY
      PID_UID.pid,
      DID_UID.did
    HAVING count(1)>2 -- X의 최저3인이상이 Y에 발생 
  )
  SELECT 
    XY.pid as from_pid, 
    media_adspots.media_site_id as to_site_id,
    XY.did AS to_did, 
    RANK() OVER (PARTITION BY XY.pid ORDER BY XY.cnt / (X.cnt * CAST(Y.cnt AS DOUBLE)) DESC ) AS ranking,
    ntile(10)  OVER (PARTITION BY XY.pid ORDER BY XY.cnt / (X.cnt * CAST(Y.cnt AS DOUBLE)) DESC ) AS grade,
    XY.cnt / (X.cnt * CAST(Y.cnt AS DOUBLE)) AS lift_x_to_y,
    XY.cnt / CAST(X.cnt AS DOUBLE) AS confident_x_to_y,
    XY.cnt / CAST(Y.cnt AS DOUBLE) AS confident_y_to_x,  
    X.cnt AS support_x, -- denominator을생략(total record)가 같으므로
    Y.cnt AS support_y, 
    XY.cnt AS support_xy 
  FROM XY
    INNER JOIN X ON XY.pid=X.pid
    INNER JOIN Y ON XY.did=Y.did
    INNER JOIN nend_master.media_adspots ON XY.did=media_adspots.id
  WHERE XY.cnt / CAST(X.cnt AS DOUBLE)> 0.00013 --confident_x_to_y
)
