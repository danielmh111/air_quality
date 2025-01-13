-- Return the mean values of PM2.5 (particulate matter <2.5 micron diameter) & VPM2.5 (volatile particulate matter <2.5 micron diameter) by each station for the year 2022 for readings taken on or near 08:00 hours (peak traffic intensity)

select 
	station_name
	,avg(pm2_5) mean_pm2_5 -- the avg() function returns the mean of the values
	,avg(vpm2_5) mean_vpm2_5
from 
	readings r 
inner join 
	stations s 
	on 
	r.site_id = s.site_id -- site id is the PK of stations and an FK in readings
where 
	year(date_time) = 2022 -- compare the year of date time to only include data in 2022
	and 
	hour(date_time) = 8 -- compare the hour of date time to only include data at peak traffic times
	-- all readings are at an exact hour, not e.g. at 7:58, so we can compare with the hour only
group by 
	station_name;