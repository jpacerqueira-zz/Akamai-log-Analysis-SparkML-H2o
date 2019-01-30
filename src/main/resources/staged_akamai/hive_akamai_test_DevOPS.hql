select date_v1,time_v2,cs_ip,cs_method,cs_uri_origin,cs_uri_stem,cs_uri_query,sc_status,sc_bytes,time_taken,cs_referer,cs_user_agent,cs_cookie,cs_host_header,cs_request_id,cached from staged_akamai.dazn_logs  where dt='2000-01-01' and hh=12 limit 10;
select dt,hh,count(*) from staged_akamai.dazn_logs where dt is not null and hh is not null  group by dt,hh ;
select dt,hh,count(*) from staged_akamai.akamai_log_ottdazn  where dt is not null and hh is not null  group by dt,hh ;
