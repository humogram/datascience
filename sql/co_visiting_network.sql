-- https://dstillery.com/wp-content/uploads/2016/07/Using-Co-Visitation-Networks.pdf
-- Referring to this research, I developed this SQL
-- you can use this for fraud traffic detection
WITH TARGET AS(
	SELECT 
		domain,
	 	nex_uid,
	 	count(1) AS cnt,
	 	1 AS user_cnt
	FROM log
	-- WHERE TIME BETWEEN A AND B
	GROUP BY nex_uid, 
),
NODE AS(
	SELECT 
		domain,
		sum(cnt) AS cnt,
		count(1) AS user_cnt
	FROM TARGET
	GROUP BY domain
),
LINKED AS(
	SELECT 
		t1.domain AS n1, 
		t2.domain AS n2, 
		sum(t1.cnt) AS overlapped_cnt,
		sum(t1.user_cnt) AS overlapped_user_cnt
	FROM TARGET t1 
	LEFT JOIN TARGET t2 on(t1.nex_uid=t2.nex_uid AND t1.domain != t2.domain)
	GROUP BY t1.domain, t2.domain
	HAVING sum(t1.user_cnt)>10 -- number of overlapped_users between linked domains more than 10
),
RESULT AS(
SELECT 
	L.*, 
	N1.user_cnt AS n1_user_cnt, 
	N2.user_cnt AS n2_user_cnt, 
	N1.cnt AS n1_cnt,
	N2.cnt AS n2_cnt,
	L.overlapped_user_cnt / N1.user_cnt  AS overlapped_user_rate,
	L.overlapped_cnt / CAST(N1.cnt AS DOUBLE) AS overlapped_imp_rate
FROM LINKED L
	LEFT JOIN NODE N1 on(L.n1=N1.domain)
	LEFT JOIN NODE N2 on(L.n2=N2.domain)
HAVING L.overlapped_user_cnt / CAST(N1.user_cnt AS DOUBLE) >  0.5
ORDER BY L.overlapped_user_cnt / CAST(N1.user_cnt AS DOUBLE) DESC
)
SELECT 
	n1, 
	count(1) AS LinkCount, 
	AVG(overlapped_user_rate) AS overlapped_user_rate_avg, 
	AVG(overlapped_imp_rate) AS overlapped_imp_rate_avg,
	AVG(n1_cnt) AS n1_cnt,
	AVG(n1_cnt) AS n1_cnt
FROM RESULT
GROUP BY n1
ORDER BY count(1) DESC