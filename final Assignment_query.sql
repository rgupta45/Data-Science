-- -------------  Question 1------------ Query time 1.42 sec----------------------------------------------------- 
SELECT CASE 
         WHEN z.dc = 1 THEN z.smblid 
       END AS 'in data set', 
       CASE 
         WHEN z.dc = 0 THEN z.smblid 
       END AS 'not in data set' 
FROM   (SELECT c.smblid, (CASE WHEN Datediff(Max(c.dateend), Min(c.datestart)) 
       >=30 
       THEN 1 ELSE 0 END ) dc 
        FROM   tweets2 c GROUP BY smblid) z; 

-- ----------------Question 2------------Query time 18.859 sec----------------------------------------------------- 
SELECT ab.ticker, 
       Round(( Sum(ab.trading_volume) / Rep_firm.rep ), 2) 
       Avg_earn_trading_vol, 
       Round(non_earn_days.trading_volume / ( total_days - Rep_firm.rep ), 2) AS 
       Avg_Nonearn_trading_vol 
FROM   (SELECT e.ticker, 
               t.datestart, 
               (SELECT Max(volumeend) - Min(volumestart) 
                FROM   tweets3 c 
                WHERE  c.datestart = t.datestart 
                       AND c.smblid = e.ticker) AS Trading_Volume 
        FROM   tweets3 t, 
               earnreldate3 e 
        WHERE  e.earnrelease_date = t.datestart 
               AND e.earnrelease_time BETWEEN t.timestart AND t.timeend 
               AND e.ticker = t.smblid 
        GROUP  BY ticker, 
                  datestart) ab, 
       (SELECT ticker, 
               Count(1) AS rep 
        FROM   earnrelmatched 
        GROUP  BY ticker) Rep_firm, 
       (SELECT ( max_volumeend_period_num.sum_voulumeend - 
                         min_volumestart_period_num.sum_voulumestart ) AS 
               Trading_volume, 
               min_volumestart_period_num.smblid, 
               max_volumeend_period_num.total_days 
        FROM   (SELECT Sum(g.volumestart) AS sum_voulumestart, 
                       abc.smblid 
                FROM   tweets3 g, 
                       (SELECT Min(efd.periodnum_inday) AS minperiod, 
                               efd.smblid, 
                               efd.datestart 
                        FROM   (SELECT c.periodnum_inday, 
                                       c.smblid, 
                                       c.datestart 
                                FROM   tweets3 c, 
                                       earnrelmatched d 
                                WHERE  c.smblid = d.smblid 
                                       AND c.datestart <> d.datestart) efd 
                        GROUP  BY smblid, 
                                  datestart) abc 
                WHERE  abc.smblid = g.smblid 
                       AND abc.datestart = g.datestart 
                       AND g.periodnum_inday = abc.minperiod 
                GROUP  BY smblid) min_volumestart_period_num, 
               (SELECT Sum(g.volumeend) AS sum_voulumeend, 
                       abcd.smblid, Datediff(Max(g.dateend), Min(g.datestart)) 
               AS 
               total_days 
                FROM   tweets3 g, 
                       (SELECT Max(efg.periodnum_inday) AS maxperiod, 
                               efg.smblid, 
                               efg.datestart 
                        FROM   (SELECT c.periodnum_inday, 
                                       c.smblid, 
                                       c.datestart 
                                FROM   tweets3 c, 
                                       earnrelmatched d 
                                WHERE  c.smblid = d.smblid 
                                       AND c.datestart <> d.datestart) efg 
                        GROUP  BY smblid, 
                                  datestart) abcd 
                WHERE  abcd.smblid = g.smblid AND abcd.datestart = g.datestart 
               AND 
               g.periodnum_inday = abcd.maxperiod GROUP BY smblid) 
               max_volumeend_period_num 
        WHERE  min_volumestart_period_num.smblid = 
               max_volumeend_period_num.smblid) 
       non_earn_days 
WHERE  ab.ticker = Rep_firm.ticker 
       AND non_earn_days.smblid = ab.ticker 
GROUP  BY ticker; 

-- ------------------------------------------------------------------------------ 
-- Question 3-----------------START--------------------------------------------------- 
-- --------------------------CHecking the query---- query time 23.047 sec-----------------------------------
SELECT vc.smblid, 
       vc.periodnum, 
       vc.max_period_num_inday, 
       vc.periodnum_inday, 
       vc.datestart, 
       vc.timestart, 
       vc.volumestart, 
       vc.volumeend, 
       Ifnull(hour_volumeend, (SELECT z.volumeend 
                               FROM   tweets3 z 
                               WHERE  z.smblid = vc.smblid 
                                      AND z.datestart = vc.datestart 
                                      AND z.periodnum_inday = 
                                          (SELECT Max(periodnum_inday) 
                                           FROM   tweets3 x 
                                           WHERE  x.smblid = z.smblid 
                                                  AND x.datestart = 
                              z.datestart))) AS 
       Next_Hour_Volumeend 
FROM   (SELECT t.periodnum, 
               t.volumeend, 
               t.periodnum_inday, 
               t.datestart, 
               t.volumestart, 
               t.smblid, 
               t.timestart, 
               (SELECT d.volumeend 
                FROM   tweets3 d 
                WHERE  d.smblid = t.smblid 
                       AND d.datestart = t.datestart 
                       AND d.periodnum_inday = t.periodnum_inday + 4) AS 
               Hour_Volumeend 
                      , 
               (SELECT Max(periodnum_inday) 
                FROM   tweets3 x 
                WHERE  x.smblid = t.smblid 
                       AND x.datestart = t.datestart)                 AS 
                      Max_period_num_inday 
        FROM   tweets3 t, 
               earnreldate3 e 
        WHERE  e.earnrelease_date = t.datestart 
               AND e.earnrelease_time BETWEEN t.timestart AND t.timeend 
               AND e.ticker = t.smblid) vc; 
               
-- -------------------------Creating table ----------------------------------
CREATE TABLE change_trade_after_earnimg_1hr 
  ( 
     smblid               VARCHAR(10), 
     periodnum            INT, 
     max_period_num_inday INT, 
     periodnum_inday      INT, 
     datestart            DATE, 
     timestart            TIME, 
     volumestart          INT, 
     volumeend            INT, 
     next_hour_volumeend  INT, 
     PRIMARY KEY (smblid, periodnum), 
     INDEX tstart (smblid, datestart, timestart) 
  ); 

SELECT * 
FROM   change_trade_after_earnimg_1hr; 
-- -----------------------Inserting value time taken : 34.375 sec--------------------

INSERT INTO change_trade_after_earnimg_1hr 
SELECT vc.smblid, 
       vc.periodnum, 
       vc.max_period_num_inday, 
       vc.periodnum_inday, 
       vc.datestart, 
       vc.timestart, 
       vc.volumestart, 
       vc.volumeend, 
       Ifnull(hour_volumeend, (SELECT z.volumeend 
                               FROM   tweets3 z 
                               WHERE  z.smblid = vc.smblid 
                                      AND z.datestart = vc.datestart 
                                      AND z.periodnum_inday = 
                                          (SELECT Max(periodnum_inday) 
                                           FROM   tweets3 x 
                                           WHERE  x.smblid = z.smblid 
                                                  AND x.datestart = 
                              z.datestart))) AS 
       Next_Hour_Volumeend 
FROM   (SELECT t.periodnum, 
               t.volumeend, 
               t.periodnum_inday, 
               t.datestart, 
               t.volumestart, 
               t.smblid, 
               t.timestart, 
               (SELECT d.volumeend 
                FROM   tweets3 d 
                WHERE  d.smblid = t.smblid 
                       AND d.datestart = t.datestart 
                       AND d.periodnum_inday = t.periodnum_inday + 4) AS 
               Hour_Volumeend 
                      , 
               (SELECT Max(periodnum_inday) 
                FROM   tweets3 x 
                WHERE  x.smblid = t.smblid 
                       AND x.datestart = t.datestart)                 AS 
                      Max_period_num_inday 
        FROM   tweets3 t, 
               earnreldate3 e 
        WHERE  e.earnrelease_date = t.datestart 
               AND e.earnrelease_time BETWEEN t.timestart AND t.timeend 
               AND e.ticker = t.smblid) vc; 

SELECT * 
FROM   change_trade_after_earnimg_1hr; 

-- -------------------Final third Query -----query time 0.047 sec----------------------
SELECT v.smblid, 
       v.periodnum, 
       v.max_period_num_inday, 
       v.periodnum_inday, 
       v.datestart, 
       v.timestart, 
       v.volumestart, 
       v.volumeend, 
       v.next_hour_volumeend, 
       CASE 
         WHEN ( v.max_period_num_inday - v.periodnum_inday ) < 4 THEN 
         ( v.next_hour_volumeend - v.volumeend ) / ( 
         v.max_period_num_inday - v.periodnum_inday ) 
         ELSE ( v.next_hour_volumeend - v.volumeend ) / 4 
       END AS 'Average change in trading volume per earning release' 
FROM   change_trade_after_earnimg_1hr v 
   
-- -END QUESTION 3----------------------------------------------------------------------------- 