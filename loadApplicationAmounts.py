from classes import CJdbcDataFrame, CSparkSession
from pyspark.sql import functions as F
objSparkSession = CSparkSession.CSparkSession('Load Application Amount', {"spark.driver.extraClassPath": "D:/postgresql-42.2.5.jar"})

strApplicationSql = '''
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
                --AND ca.property_id = 580913
                AND ca.updated_on::DATE > ( CURRENT_DATE - 60 )
                AND ca.application_status_id NOT IN ( 5, 6 ) ) AS temp_application
            '''
objDataFrameApplication = CJdbcDataFrame.CJdbcDataFrame(objSparkSession.getSession(), strApplicationSql)
applicationDataFrame = objDataFrameApplication.getDataFrame()

strScheduledChargeSql = '''
                        ( SELECT
                            a.cid,
                            a.id AS application_id,
                            a.lease_id,
                            a.lease_interval_id,
                            sc.ar_trigger_id,
                            at.ar_trigger_type_id,
                            sc.ar_origin_id,
                            sc.ar_code_type_id,
                            sc.charge_amount,
                            sc.lease_term_months,
                            sc.id As scheduled_charge_id,
                            pt.default_pet_type_id,
                            aog.add_on_category_id,
                            sc.charge_start_date,
                            COALESCE( sc.charge_end_date, '2099/12/31'::DATE ) As charge_end_date
                        FROM
                            
                            applications AS a
                            JOIN lease_intervals li ON
                                a.cid = li.cid
                                AND a.lease_interval_id = li.id
                                AND a.lease_approved_on IS NOT NULL
                                AND ( li.lease_status_type_id <> 6  )
                            JOIN scheduled_charges AS sc ON
                                a.cid = sc.cid
                                AND a.lease_interval_id = sc.lease_interval_id
                            JOIN ar_triggers AS at ON
                                sc.ar_trigger_id = at.id
                            LEFT JOIN lease_customers AS lc ON
                                sc.cid = lc.cid
                                AND sc.lease_id = lc.lease_id
                                AND sc.customer_id = lc.customer_id
                            LEFT JOIN pet_types AS pt ON
                                sc.cid = pt.cid
                                AND sc.ar_origin_reference_id = pt.id
                                AND sc.ar_origin_id = 3
                            LEFT JOIN add_ons AS ao ON
                                sc.cid = ao.cid
                                AND sc.ar_origin_reference_id = ao.id
                                AND sc.ar_origin_id = 4
                            LEFT JOIN add_on_groups AS aog ON
                                ao.cid = aog.cid
                                AND ao.add_on_group_id = aog.id
                        WHERE a.cid = 14437
                            --AND a.property_id = 580913
                            AND sc.deleted_on IS NULL
                            AND sc.is_unselected_quote = FALSE
                            AND COALESCE( lc.customer_type_id, 1 ) = 1 ) As sc
                        '''


objDataScheduledCharge = CJdbcDataFrame.CJdbcDataFrame(objSparkSession.getSession(), strScheduledChargeSql)
scheduledChargeDataFrame = objDataScheduledCharge.getDataFrame()


joinDataFrame = scheduledChargeDataFrame.alias('sc').join( applicationDataFrame.alias('a'), ( applicationDataFrame.id == scheduledChargeDataFrame.application_id ) & ( applicationDataFrame.cid == scheduledChargeDataFrame.cid ) ).filter( F.col("executed_rent_date").between( F.col("charge_start_date"), F.col("charge_end_date") ) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_base',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ) & ( joinDataFrame.ar_origin_id == 1 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_amenity',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ) & ( joinDataFrame.ar_origin_id == 2 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_pet',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ) & ( joinDataFrame.ar_origin_id == 3 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_add_on',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ) & ( joinDataFrame.ar_origin_id == 4 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_risk_premium',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ) & ( joinDataFrame.ar_origin_id == 5 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_special',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ) & ( joinDataFrame.ar_origin_id == 6 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_rent_total',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id == 2 ), joinDataFrame.charge_amount  ).otherwise(0) )


joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_base',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ) & ( joinDataFrame.ar_origin_id == 1 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_amenity',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ) & ( joinDataFrame.ar_origin_id == 2 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_pet',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ) & ( joinDataFrame.ar_origin_id == 3 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_add_on',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ) & ( joinDataFrame.ar_origin_id == 4 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_risk_premium',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ) & ( joinDataFrame.ar_origin_id == 5 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_special',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ) & ( joinDataFrame.ar_origin_id == 6 ), joinDataFrame.charge_amount  ).otherwise(0) )
joinDataFrame = joinDataFrame.withColumn('executed_monthly_other_total',F.when(( joinDataFrame.ar_trigger_type_id == 3 ) & ( joinDataFrame.ar_code_type_id != 2 ), joinDataFrame.charge_amount  ).otherwise(0) )


joinDataFrame = joinDataFrame.groupBy("sc.cid","sc.lease_interval_id").agg(F.sum('executed_monthly_rent_base').alias('executed_monthly_rent_base'),
                                                                           F.sum('executed_monthly_rent_amenity').alias('executed_monthly_rent_amenity'),
                                                                           F.sum('executed_monthly_rent_pet').alias('executed_monthly_rent_pet'),
                                                                           F.sum('executed_monthly_rent_add_on').alias('executed_monthly_rent_amenity'),
                                                                           F.sum('executed_monthly_rent_risk_premium').alias('executed_monthly_rent_add_on'),
                                                                           F.sum('executed_monthly_rent_special').alias('executed_monthly_rent_risk_premium'),
                                                                           F.sum('executed_monthly_rent_total').alias('executed_monthly_rent_special'),
                                                                           F.sum('executed_monthly_other_base').alias('executed_monthly_other_base'),
                                                                           F.sum('executed_monthly_other_amenity').alias('executed_monthly_other_amenity'),
                                                                           F.sum('executed_monthly_other_pet').alias('executed_monthly_other_pet'),
                                                                           F.sum('executed_monthly_other_add_on').alias('executed_monthly_other_add_on'),
                                                                           F.sum('executed_monthly_other_risk_premium').alias('executed_monthly_other_risk_premium'),
                                                                           F.sum('executed_monthly_other_special').alias('executed_monthly_other_special'),
                                                                           F.sum('executed_monthly_other_total').alias('executed_monthly_other_total'),

                                                                           )

joinDataFrame.show()
