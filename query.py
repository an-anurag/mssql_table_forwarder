
start_query = "SELECT TOP 1 CONVERT(int,event_id) AS event_id FROM {} ORDER BY event_id DESC"

main_query = """
                SELECT CONVERT(varchar(200),event_id), CONVERT(varchar(200), IPAddress), CAST(descr AS TEXT),
                CONVERT(varchar(200),event_type), CONVERT(varchar(200),severity), 
                CONVERT(varchar(200),host_display_name), CONVERT(varchar(200),par1), CONVERT(varchar(200),par3), 
                CONVERT(varchar(200),rise_time), CONVERT(varchar(200),par2), CONVERT(varchar(200),par4), 
                CONVERT(varchar(200),par5), CONVERT(varchar(200),par6), CONVERT(varchar(200),par7)
                FROM {} WHERE (descr LIKE '%Blocked%' OR descr LIKE '%DETECTED%' OR descr
                LIKE '%QUARANTINED%' OR descr LIKE '%DELETED%') AND event_id > CONVERT(bigint,'{}')
                ORDER BY event_id
                """
