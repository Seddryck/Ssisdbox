USE [SSISDB]
GO

/****** Object:  StoredProcedure [custom].[cleanup_server_log_status]    Script Date: 7/07/2017 12:58:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [custom].[cleanup_server_log_status]
as

set transaction isolation level read uncommitted
declare @cleanup_job_id bigint;
select @cleanup_job_id=max([cleanup_job_id]) from [custom].[cleanup_logs]

declare @total float;
declare @delete_row_count bigint;
declare @timespan time;
declare @start_time datetime2;

select
	@total=count(*)
	, @start_time=MIN(start_time)
	, @delete_row_count=sum(delete_row_count)
	, @timespan = cast(cast(max(end_time) as datetime)-cast(min(start_time) as datetime) as time)
from
	[custom].[cleanup_logs]
where
	cleanup_job_id=@cleanup_job_id

declare @successful float;
declare @average_timespan time;
declare @max_timespan time;
declare @min_timespan time;

select
	@successful=isnull(count(*),0)
	, @average_timespan = dateadd(millisecond, avg(datediff(millisecond,0,cast(delete_timespan as datetime))),0)
	, @max_timespan = dateadd(millisecond, max(datediff(millisecond,0,cast(delete_timespan as datetime))),0)
	, @min_timespan = dateadd(millisecond, min(datediff(millisecond,0,cast(delete_timespan as datetime))),0)
from
	[custom].[cleanup_logs]
where
	cleanup_job_id=@cleanup_job_id
	and [state]='Successful'

select 
	@cleanup_job_id as cleanup_job_id
	, @total as operation_count
	, @successful as operation_executed_count
	, round((@successful/@total)*100, 2) as operation_executed_percentage
	, @timespan as timespan
	, @start_time as start_time
	, dateadd(millisecond, datediff(millisecond,0,cast(@timespan as datetime)) / ((@successful/@total)) ,@start_time) as expected_end_time
	, @delete_row_count as delete_row_count
	, @delete_row_count / datediff(second,0,cast(@timespan as datetime)) as [delete_row_count/sec]
	, @average_timespan as average_timespan
	, @min_timespan as min_timespan
	, @max_timespan as max_timespan

return 0

GO


