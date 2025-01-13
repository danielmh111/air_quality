-- Extend the previous query to show these values for all stations for all the data

select 
	station_name
	,avg(pm2_5)
	,avg(vpm2_5)
from 
	readings r
inner join 
	stations s 
	on 
	r.site_id = s.site_id 
-- there is no where clause since this is for all the data
group by 
	station_name; 