select name, location, org_class count_Traces, avg_rtt from monitors
right join (
	select monitor_name, count(*) as count_traces, avg(rtt) as avg_rtt
	from traces
	group by monitor_name
) traces on traces.monitor_name = monitors.name
order by avg_rtt desc

