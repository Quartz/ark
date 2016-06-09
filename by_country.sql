select
	country,
	count(*),
	round(avg(rtt)::numeric, 1) as avg_rtt,
	round(avg(ip_hops), 1) as avg_ip_hops,
	round(avg(as_hops), 1) as avg_as_hops
from traces
group by country
order by avg_as_hops desc
