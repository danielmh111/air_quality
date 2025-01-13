-- Return the date/time, station name and the highest recorded value of nitrogen oxide (NOx) found in the dataset for the year 2022

select
	r.date_time
	,s.station_name
	,r.NOx
from 
	readings r 
inner join 
	stations s 
	on 
	r.site_id = s.site_id -- this is the PK of stations and a FK in readings
where 
	r.NOx  = ( -- find the highest NOx reading in 2022
				select 
					max(NOx)
				from 
					readings 
				where -- use the year() function to only compare the year
					year(date_time) = 2022
			);