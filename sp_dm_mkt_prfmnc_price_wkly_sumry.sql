
DECLARE 
 
run_date DATE;
pl_export_fisc_dt DATE;
ampps_master_dt DATE;

BEGIN

DROP TABLE IF EXISTS pl_export;
CREATE TEMP TABLE pl_export AS (
    WITH export AS (
        SELECT 
            LPAD(plan_to_nbr, 10, '0') AS plan_to_nbr 
            ,fisc_yr
            ,fisc_qtr		  
            ,fisc_pd
            ,(fisc_yr::VARCHAR || LPAD(fisc_qtr::VARCHAR, 3, '0'))::INT AS fisc_yr_qtr
            ,(fisc_yr::VARCHAR || LPAD(fisc_pd::VARCHAR, 3, '0'))::INT AS fisc_yr_pd
            ,catg_nm
            ,dp_bu_cd
            ,val_curr_cd
            ,CASE
                WHEN catg_nm IN ('COLD CEREAL','CEREAL','CEREAL ALTERNATIVES','HOT CEREAL','OTHER CONVENIENCE','RTEC') AND (dp_bu_cd = 'K1') THEN 'CEREAL'
                WHEN catg_nm IN ('COOKIES','INGREDIENTS') AND (dp_bu_cd = 'K1') THEN 'COOKIES & OTHER'
                WHEN catg_nm IN ('HANDHELD','SYRUP CARRIERS','FROZEN BREAKFAST') AND (dp_bu_cd = 'K1')  THEN 'FROZEN BREAKFAST'
                WHEN catg_nm IN
                ('BARS','CEREAL ALTERNATIVES','COLD CEREAL','CEREAL','COOKIES','CRACKERS','DRINKABLE BREAKFAST','FROZEN MEALS','FRUIT SNACKS','H&W BEVERAGES','HANDHELD'
                ,'HOT CEREAL','INGREDIENTS','MEAT SNACK ALTERNATIVES','FROZEN BREAKFAST','OTHER CONVENIENCE','PLANT PROTEIN ADJACENCY','SALTY SNACKS','SYRUP CARRIERS','TOASTER PASTRIES','VEGGIE') 
                AND ((dp_bu_cd = 'N&I') OR (dp_bu_cd = 'KASHI') OR (dp_bu_cd = 'INSURGENT BRANDS')) THEN 'NATURAL & INSURGENT'
                WHEN catg_nm IN ('BARS','DRINKABLE BREAKFAST','FRUIT SNACKS','H&W BEVERAGES','TOASTER PASTRIES','BEVERAGES') AND (dp_bu_cd = 'K1') THEN 'PWS'
                WHEN catg_nm IN ('CRACKERS','SALTY SNACKS') AND (dp_bu_cd = 'K1') THEN 'SALTY SNACKING'
                WHEN catg_nm IN ('FROZEN MEALS','PLANT PROTEIN ADJACENCY','VEGGIE') AND (dp_bu_cd = 'K1') THEN 'VEGGIE'
            END AS comrcl_catg_cd  
            ,ytd_ytg_cd
            ,meas_nm
            ,src_nm
            ,SUM(CASE WHEN meas_nm = 'NET SALES' THEN actl_meas_val::FLOAT ELSE 0.0 END) AS actl_nsv				  
            ,SUM(CASE WHEN meas_nm = 'NET SALES' THEN actl_yr_ago_meas_val::FLOAT ELSE 0.0 END) AS yr_ago_actl_nsv	
            ,SUM(CASE WHEN meas_nm = 'GROSS SALES' THEN actl_meas_val::FLOAT ELSE 0.0 END) AS actl_gsv 				  
            ,SUM(CASE WHEN meas_nm = 'GROSS SALES' THEN actl_yr_ago_meas_val::FLOAT ELSE 0.0 END) AS yr_ago_actl_gsv		         	  			  
            ,SUM(CASE WHEN meas_nm = 'KILOS' THEN actl_meas_val::FLOAT ELSE 0.0 END) as actl_net_kg_val 
            ,SUM(CASE WHEN meas_nm = 'KILOS' THEN actl_yr_ago_meas_val::FLOAT ELSE 0.0 END) as yr_ago_actl_net_kg_val        
            ,SUM(CASE WHEN meas_nm = 'GROSS PROFIT 2' THEN actl_meas_val::FLOAT ELSE 0.0 END) AS actl_gross_prft_val 				  
            ,SUM(CASE WHEN meas_nm = 'GROSS PROFIT 2' THEN actl_yr_ago_meas_val::FLOAT ELSE 0.0 END) AS yr_ago_actl_gross_prft_val	  			  
            ,SUM(CASE WHEN meas_nm = 'TRADE' THEN actl_meas_val::FLOAT ELSE 0.0 END) AS actl_trade_val 				  
            ,SUM(CASE WHEN meas_nm = 'TRADE' THEN actl_yr_ago_meas_val::FLOAT ELSE 0.0 END) AS yr_ago_trade_val
        FROM sales_prfmnc_eval.sales_rptg_sales_trkr
	    WHERE dp_bu_cd IN ('K1', 'KASHI', 'N&I', 'INSURGENT BRANDS')
        GROUP BY  
            LPAD(plan_to_nbr, 10, '0')
            ,fisc_yr
            ,fisc_qtr		  
            ,fisc_pd
            ,(fisc_yr::VARCHAR || LPAD(fisc_qtr::VARCHAR, 3, '0'))::INT 
            ,(fisc_yr::VARCHAR || LPAD(fisc_pd::VARCHAR, 3, '0'))::INT 
            ,catg_nm
            ,dp_bu_cd
            ,val_curr_cd
            ,ytd_ytg_cd
            ,meas_nm
            ,src_nm
    )
    SELECT 
        a.*
        ,CASE WHEN b.plan_to_nm LIKE 'C&S%' THEN 'C&S' ELSE UPPER(b.plan_to_nm) END AS plan_to_nm
    FROM export a
    LEFT JOIN (
        SELECT DISTINCT plan_to_nbr, plan_to_nm FROM cust_mstr.cust_sold_to_pivot
        WHERE TRIM(tdlinx_nbr) != ''
        -- Filtering the channel name
        AND UPPER(hier_d_sales_mgmt_a_nm) NOT IN ('CANADA L3', 'REMARKETING', 'PUREPLAY E-COMMERCE', 'SPECIALTY')
    ) b
    ON a.plan_to_nbr = b.plan_to_nbr
);

DROP TABLE IF EXISTS pl_export_timeframes;
CREATE TEMP TABLE pl_export_timeframes AS (
    SELECT 
        plan_to_nm
        ,plan_to_nbr
        ,'QTD' AS tmfrm_cd
        ,catg_nm
        ,dp_bu_cd
        ,comrcl_catg_cd
        ,src_nm
        ,SUM(actl_nsv)                          AS actl_nsv       				  
        ,SUM(yr_ago_actl_nsv)                   AS yr_ago_actl_nsv  	
        ,SUM(actl_gsv)                          AS actl_gsv     				
        ,SUM(yr_ago_actl_gsv)                   AS yr_ago_actl_gsv
        ,SUM(actl_net_kg_val)                   AS actl_net_kg_val          
        ,SUM(yr_ago_actl_net_kg_val)            AS yr_ago_actl_net_kg_val          
        ,SUM(actl_gross_prft_val)               AS actl_gross_prft_val          				
        ,SUM(yr_ago_actl_gross_prft_val)        AS yr_ago_actl_gross_prft_val   	
        ,SUM(actl_trade_val)                    AS actl_trade_val       				  
        ,SUM(yr_ago_trade_val)                  AS yr_ago_trade_val
    FROM pl_export
    WHERE ytd_ytg_cd = 'YTD' AND fisc_yr_qtr = (SELECT MAX(fisc_yr_qtr) FROM pl_export WHERE ytd_ytg_cd = 'YTD')
	GROUP BY 
        plan_to_nm
        ,plan_to_nbr
        ,tmfrm_cd
        ,catg_nm
        ,dp_bu_cd
        ,comrcl_catg_cd
        ,src_nm
    
    UNION ALL

    SELECT 
        plan_to_nm
        ,plan_to_nbr
        ,'YTD' AS tmfrm_cd
        ,catg_nm
        ,dp_bu_cd
        ,comrcl_catg_cd
        ,src_nm
        ,SUM(actl_nsv)                          AS actl_nsv       				  
        ,SUM(yr_ago_actl_nsv)                   AS yr_ago_actl_nsv  	
        ,SUM(actl_gsv)                          AS actl_gsv     				
        ,SUM(yr_ago_actl_gsv)                   AS yr_ago_actl_gsv
        ,SUM(actl_net_kg_val)                   AS actl_net_kg_val          
        ,SUM(yr_ago_actl_net_kg_val)            AS yr_ago_actl_net_kg_val          
        ,SUM(actl_gross_prft_val)               AS actl_gross_prft_val          				
        ,SUM(yr_ago_actl_gross_prft_val)        AS yr_ago_actl_gross_prft_val   	
        ,SUM(actl_trade_val)                    AS actl_trade_val       				  
        ,SUM(yr_ago_trade_val)                  AS yr_ago_trade_val
    FROM pl_export
    WHERE ytd_ytg_cd = 'YTD' AND fisc_yr = (SELECT MAX(fisc_yr) FROM pl_export WHERE ytd_ytg_cd = 'YTD')
	GROUP BY 
        plan_to_nm
        ,plan_to_nbr
        ,tmfrm_cd
        ,catg_nm
        ,dp_bu_cd
        ,comrcl_catg_cd
        ,src_nm
);

SELECT MIN(fisc_dt) INTO run_date 
FROM (
    SELECT MAX(fisc_dt) AS fisc_dt FROM sales_prfmnc_eval.mkt_prfmnc_pos_store_gtin WHERE src_nm LIKE '%e2open%'
    UNION
    SELECT MAX(fisc_dt) AS fisc_dt FROM sales_prfmnc_eval.mkt_prfmnc_pos_store_gtin WHERE src_nm LIKE '%nielsen%'
);

-- POS Data for the current year
DROP TABLE IF EXISTS pos_sales_curr;
CREATE TEMP TABLE pos_sales_curr AS (
    SELECT 
        src_nm
        ,LEFT(fisc_yr_wk, 4) AS fisc_yr 
        ,RIGHT(fisc_yr_wk, 3) AS fisc_wk
        ,retlr_nm
        ,sold_to_nbr
        ,gtin
        ,gtin_type_cd
        ,SUM(sale_val) AS sale_val
        ,SUM(sale_qty) AS sale_qty
        ,SUM(sale_vol_lb_val) AS sale_vol_lb_val
    -- FROM (SELECT * FROM sales_prfmnc_eval.mkt_prfmnc_pos_store_gtin LIMIT 100)
    FROM sales_prfmnc_eval.mkt_prfmnc_pos_store_gtin
    WHERE fisc_dt BETWEEN run_date - INTERVAL '59 WEEK' AND run_date
    GROUP BY 
        src_nm
        ,retlr_nm
        ,fisc_yr
        ,fisc_wk
        ,sold_to_nbr
        ,gtin
        ,gtin_type_cd
);

DROP TABLE IF EXISTS pos_sales_ly;
CREATE TEMP TABLE pos_sales_ly AS (
    SELECT 
        src_nm
        ,retlr_nm
        ,LEFT(fisc_yr_wk, 4) AS fisc_yr 
        ,RIGHT(fisc_yr_wk, 3) AS fisc_wk
        ,sold_to_nbr
        ,gtin
        ,gtin_type_cd
        ,SUM(sale_val) AS yr_ago_sale_val
        ,SUM(sale_qty) AS yr_ago_sale_qty
        ,SUM(sale_vol_lb_val) AS yr_ago_sale_vol_lb_val
    -- FROM (SELECT * FROM sales_prfmnc_eval.mkt_prfmnc_pos_store_gtin LIMIT 100)
    FROM sales_prfmnc_eval.mkt_prfmnc_pos_store_gtin
    WHERE fisc_dt BETWEEN run_date - INTERVAL '115 WEEK' AND run_date - INTERVAL '52 WEEK'
    GROUP BY 
        src_nm
        ,fisc_yr
        ,fisc_wk
        ,retlr_nm
        ,sold_to_nbr
        ,gtin
        ,gtin_type_cd
);

DROP TABLE IF EXISTS pos_sales_comb;
CREATE TEMP TABLE pos_sales_comb AS (
    SELECT
         COALESCE(a.src_nm, b.src_nm) AS src_nm
        ,COALESCE(a.fisc_yr, (b.fisc_yr::INT+1)::TEXT) AS fisc_yr
        ,COALESCE(a.fisc_wk, b.fisc_wk) AS fisc_wk
        ,COALESCE(a.retlr_nm, b.retlr_nm) AS retlr_nm
        ,COALESCE(a.sold_to_nbr, b.sold_to_nbr) AS sold_to_nbr
        ,COALESCE(a.gtin , b.gtin) AS gtin
        ,COALESCE(a.gtin_type_cd, b.gtin_type_cd) AS gtin_type_cd
        ,a.sale_val
        ,a.sale_qty
        ,a.sale_vol_lb_val
        ,b.yr_ago_sale_val
        ,b.yr_ago_sale_qty
        ,b.yr_ago_sale_vol_lb_val
        ,CASE WHEN a.fisc_wk IS NULL THEN 'LY' WHEN b.fisc_wk IS NULL THEN 'CUR' ELSE 'BOTH' END AS avail
    FROM pos_sales_curr a
    FULL OUTER JOIN pos_sales_ly b 
    ON  a.src_nm = b.src_nm 
        AND a.retlr_nm = b.retlr_nm
        AND a.sold_to_nbr = b.sold_to_nbr
        AND a.gtin = b.gtin
        AND a.fisc_yr::INT - 1 = b.fisc_yr::INT
        AND a.fisc_wk = b.fisc_wk
);

-- Combining the fisc week end date information
DROP TABLE IF EXISTS pos_final;
CREATE TEMP TABLE pos_final AS (
    WITH pos_cust AS (
        SELECT 
            pos.*
            ,CASE
                WHEN retlr_nm NOT IN ('C&S', 'DOLLAR GENERAL', 'SAMS CLUB') THEN cust.sold_to_nm
                ELSE NULL
            END AS sold_to_nm
            ,CASE
                WHEN retlr_nm NOT IN ('C&S', 'DOLLAR GENERAL', 'SAMS CLUB') THEN cust.plan_to_nbr
                ELSE ISNULL(cust_pln_to.plan_to_nbr, '')
            END AS plan_to_nbr
            ,CASE
                WHEN retlr_nm NOT IN ('C&S', 'DOLLAR GENERAL', 'SAMS CLUB') THEN cust.plan_to_nm
                ELSE retlr_nm
            END AS plan_to_nm
        FROM pos_sales_comb pos
        LEFT JOIN cust_mstr.cust_sold_to_pivot cust
            ON pos.sold_to_nbr = cust.sold_to_nbr
        LEFT JOIN (
            SELECT DISTINCT plan_to_nbr, plan_to_nm FROM cust_mstr.cust_sold_to_pivot
            WHERE TRIM(tdlinx_nbr) != ''
            -- Filtering the channel name
            AND UPPER(hier_d_sales_mgmt_a_nm) NOT IN ('CANADA L3', 'REMARKETING', 'PUREPLAY E-COMMERCE', 'SPECIALTY')
        ) cust_pln_to
            ON retlr_nm = cust_pln_to.plan_to_nm
            AND retlr_nm IN ('C&S', 'DOLLAR GENERAL', 'SAMS CLUB')
            WHERE TRIM(tdlinx_nbr) != ''
            -- Filtering the channel name
            AND UPPER(hier_d_sales_mgmt_a_nm) NOT IN ('CANADA L3', 'REMARKETING', 'PUREPLAY E-COMMERCE', 'SPECIALTY')
    )
    ,pos_cust_matrl AS (
        SELECT 
            pos.*
            ,UPPER(matrl.catg_nm) AS catg_nm
            ,UPPER(matrl.dp_bu_cd) AS dp_bu_cd
            ,UPPER(matrl.comrcl_catg_cd) AS comrcl_catg_cd
        FROM pos_cust pos
        LEFT JOIN matrl_mstr.gtin matrl
            ON pos.gtin = matrl.gtin
            AND UPPER(matrl.comrcl_catg_cd) != 'WIP'
    )
    SELECT 
        pos.*
        ,fisc_wk_end_dt
    FROM pos_cust_matrl pos
    LEFT JOIN fin_acctg_ops.ref_fisc_cal_wk fcal
        ON (pos.fisc_yr || pos.fisc_wk) = fcal.fisc_yr_wk
);


SELECT MAX(fisc_wk_end_dt) INTO pl_export_fisc_dt
FROM fin_acctg_ops.ref_fisc_cal_wk
WHERE fisc_yr_pd = (SELECT MAX(fisc_yr_pd) FROM pl_export WHERE ytd_ytg_cd='YTD');


DROP TABLE IF EXISTS all_data;
CREATE TEMP TABLE all_data AS (
    SELECT 
        'SALES_PERFORMANCE' AS sectn_nm
        ,'NA' AS sold_to_nbr
        ,'NA' AS sold_to_nm
        ,pl_export_fisc_dt AS fisc_wk_end_dt
        ,pl_export_fisc_dt AS latst_data_avail_dt
        ,plan_to_nm
        ,plan_to_nbr
        ,tmfrm_cd
        ,catg_nm
        ,dp_bu_cd
        ,comrcl_catg_cd
        ,src_nm
        ,actl_nsv				  
        ,yr_ago_actl_nsv	
        ,actl_gsv 				
        ,yr_ago_actl_gsv	
        ,actl_net_kg_val 
        ,yr_ago_actl_net_kg_val   
        ,(actl_net_kg_val * 2.20462) AS actl_net_lb_val
        ,(yr_ago_actl_net_kg_val * 2.20462) AS yr_ago_actl_net_lb_val  
        ,actl_gross_prft_val 				
        ,yr_ago_actl_gross_prft_val 
        ,actl_trade_val 				  
        ,yr_ago_trade_val
        ,0.0 AS sale_val
        ,0.0 AS yr_ago_sale_val
        ,0 AS sale_qty
        ,0 AS yr_ago_sale_qty
        ,0.0 AS sale_vol_lb_val
        ,0.0 AS yr_ago_sale_vol_lb_val
    FROM pl_export_timeframes
    -- Filtering the inactive plan to and stores
    WHERE UPPER(plan_to_nm) NOT LIKE '%INACTIVE%' AND UPPER(sold_to_nm) NOT LIKE '%INACTIVE%'

    UNION ALL 
    
    SELECT 
        'SALES_VALUE' AS sectn_nm
        ,sold_to_nbr
        ,sold_to_nm
        ,fisc_wk_end_dt
        ,run_date AS latst_data_avail_dt
        ,plan_to_nm
        ,plan_to_nbr
        ,'NA' AS tmfrm_cd
        ,catg_nm
        ,dp_bu_cd
        ,comrcl_catg_cd
        ,src_nm
        ,0.0 AS actl_nsv				  
        ,0.0 AS yr_ago_actl_nsv 
        ,0.0 AS actl_gsv 				
        ,0.0 AS yr_ago_actl_gsv 
        ,0.0 AS actl_net_kg_val 
        ,0.0 AS yr_ago_actl_net_kg_val  
        ,0.0 AS actl_net_lb_val 
        ,0.0 AS yr_ago_actl_net_lb_val        
        ,0.0 AS actl_gross_prft_val 				
        ,0.0 AS yr_ago_actl_gross_prft_val	
        ,0.0 AS actl_trade_val 				  
        ,0.0 AS yr_ago_trade_val
        ,COALESCE(sale_val, 0) AS sale_val
        ,COALESCE(yr_ago_sale_val, 0) AS yr_ago_sale_val
        ,COALESCE(sale_qty, 0) AS sale_qty
        ,COALESCE(yr_ago_sale_qty, 0) AS yr_ago_sale_qty
        ,COALESCE(sale_vol_lb_val, 0) AS sale_vol_lb_val
        ,COALESCE(yr_ago_sale_vol_lb_val, 0) AS yr_ago_sale_vol_lb_val
    FROM pos_final
    -- Filtering the inactive plan to and stores
    WHERE UPPER(plan_to_nm) NOT LIKE '%INACTIVE%' AND UPPER(sold_to_nm) NOT LIKE '%INACTIVE%'
);


DELETE FROM sales_prfmnc_eval.dm_mkt_prfmnc_price_wkly_sumry;
INSERT INTO sales_prfmnc_eval.dm_mkt_prfmnc_price_wkly_sumry (
    SELECT         
        sectn_nm
        ,tmfrm_cd
        ,fisc_wk_end_dt
        ,latst_data_avail_dt
        ,sold_to_nm
        ,sold_to_nbr
        ,ISNULL(plan_to_nm, '') AS plan_to_nm
        ,plan_to_nbr
        ,COALESCE(catg_nm, 'NA') AS catg_nm
        ,COALESCE(dp_bu_cd, 'NA') AS dp_bu_cd
        ,COALESCE(comrcl_catg_cd, 'NA') AS comrcl_catg_cd
        ,actl_nsv				  
        ,yr_ago_actl_nsv	
        ,actl_gsv 				
        ,yr_ago_actl_gsv	
        ,actl_net_kg_val 
        ,yr_ago_actl_net_kg_val 	
        ,actl_net_lb_val 
        ,yr_ago_actl_net_lb_val        
        ,actl_gross_prft_val 				
        ,yr_ago_actl_gross_prft_val 
        ,actl_trade_val 				  
        ,yr_ago_trade_val
        ,sale_val
        ,yr_ago_sale_val
        ,sale_qty
        ,yr_ago_sale_qty
        ,sale_vol_lb_val
        ,yr_ago_sale_vol_lb_val
        ,src_nm
        ,MD5(src_nm || sectn_nm || fisc_wk_end_dt || tmfrm_cd || plan_to_nbr || plan_to_nm || catg_nm || dp_bu_cd || comrcl_catg_cd) AS hash_key       
        ,CURRENT_TIMESTAMP AS kortex_dprct_ts
        ,CURRENT_TIMESTAMP AS kortex_upld_ts 
        ,CURRENT_TIMESTAMP AS kortex_cre_ts  
        ,CURRENT_TIMESTAMP AS kortex_updt_ts 
    FROM all_data
    WHERE plan_to_nbr IS NOT NULL
);

EXCEPTION WHEN OTHERS THEN ROLLBACK;

COMMIT;

END; 
