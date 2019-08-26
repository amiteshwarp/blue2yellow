from classes import CJdbcDataFrame, CSparkSession
objSparkSession = CSparkSession.CSparkSession('Load Application Amount', {"spark.driver.extraClassPath": "D:/postgresql-42.2.5.jar"})
#strSql1 = '( SELECT * FROM application_stages ) as stages'
strSql = '''
            (
            SELECT
                ca.id,
                ca.cid,
                ca.property_id,
                ca.unit_type_id,
                ca.unit_space_id,
                ca.lease_interval_id,
                ca.lease_start_window_id,
                COALESCE( ca.space_configuration_id, 0 ),
                cl.lease_interval_type_id,
                cl.lease_status_type_id,
                ca.application_stage_id,
                ca.application_status_id,
                ca.lease_approved_on::DATE AS lease_approved_on,
                COALESCE( ca.application_started_on, ca.application_datetime )::DATE AS move_in_date,
                cl.is_month_to_month::BOOLEAN,
        
                -- calculate the lease term months from lease dates, if term_month not set on the application
                COALESCE(
                    NULLIF( cl.is_month_to_month, 0 ),
                    NULLIF( ca.term_month, 0 ),
                    NULLIF(
                        EXTRACT(year from age(cl.lease_end_date, cl.lease_start_date)) * 12 +
                        EXTRACT(month from age(cl.lease_end_date, cl.lease_start_date)), 0 ), 12 )::INTEGER AS lease_term_months,
        
                CASE WHEN ( ca.lease_approved_on IS NULL OR ca.details->>'advertised_amounts_loaded_on' IS NULL )
                    THEN TRUE
                    ELSE FALSE
                END AS load_advertised_amount,
                date_trunc('MONTH',ca.lease_start_date + INTERVAL '1 MONTH' )::DATE As executed_rent_date -- date required to calculate accurate executed_rent
            FROM
                cached_applications AS ca
                JOIN cached_leases AS cl ON
                    ca.cid = cl.cid
                    AND ca.lease_id = cl.id
            WHERE
                cl.cid = 14437
                AND ca.property_id = 580913
                AND ca.updated_on::DATE > ( CURRENT_DATE - 60 )
                AND ca.application_status_id NOT IN ( 5, 6 ) ) AS temp_application
            '''
objDataFrame = CJdbcDataFrame.CJdbcDataFrame(objSparkSession.getSession(), strSql)
df = objDataFrame.getDataFrame()
df.printSchema()
df.show()