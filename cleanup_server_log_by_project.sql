USE [SSISDB]
GO

/****** Object:  StoredProcedure [custom].[cleanup_server_log_by_project]    Script Date: 7/07/2017 12:57:44 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [custom].[cleanup_server_log_by_project]
(
	@project_name varchar(200)
	, @retention_days int
) as

update
	[custom].[cleanup_logs]
set
	[state]='Failed'
where
	[state]='Waiting'
	and project_name=@project_name;

declare @cleanup_job_id bigint;
select @cleanup_job_id=coalesce(MAX([cleanup_job_id]), 0)+1 from [custom].[cleanup_logs];

insert into
    [custom].[cleanup_logs]
(
    [object_id],
	[project_name],
	[operation_id],
	[cleanup_job_id],
	[cleanup_job_start_time],
	[state]
)
select
	o.object_id,
	@project_name,
	o.operation_id,
	@cleanup_job_id,
	getdate(),
	'Waiting'
from
	[internal].[operations] o
inner join
	[internal].[projects] p
	on o.object_id = p.project_id
		and o.operation_type=200
where
	p.name = @project_name
	and o.end_time<dateadd(day, -@retention_days, getdate())
	and o.[status]<>-1

print cast(@@rowcount as varchar) + ' operations to cleanup';

declare @operation_id bigint;
declare operationCursor cursor for
	select
		operation_id
	from
		[custom].[cleanup_logs]
	where
		cleanup_job_id=@cleanup_job_id;

open operationCursor  
fetch next from operationCursor into @operation_id; 

declare @fetch_status int;
set @fetch_status = @@fetch_status

if @@fetch_status <> 0   
begin
        print 'No operation to cleanup';
		return;  
end
else
begin
	print 'Setting auto-updates to off';
	exec sp_autostats '[internal].[event_message_context]', 'Off';
	exec sp_autostats '[internal].[event_messages]', 'Off';
	exec sp_autostats '[internal].[operation_messages]', 'Off';
end

begin try
	while @fetch_status = 0
	begin
		declare @delete_row_count bigint;
		declare @delete_statement_count bigint;
		declare @start_time datetime2;
		declare @delete_batch_size bigint;

		set @start_time = getdate();
		set @delete_batch_size = 1000;

		print 'Cleaning operation ' + cast(@operation_id as varchar) + ' at ' + cast(@start_time as varchar)

		update
			[custom].[cleanup_logs]
		set
			[state]='Running',
			[start_time]=@start_time
		where
			[cleanup_job_id]=@cleanup_job_id
			and [operation_id]=@operation_id;

		declare @rows bigint

		/****event_message_context****/

		set @rows = @delete_batch_size;
		set @delete_row_count=0;
		set @delete_statement_count=0;

		while(@rows=@delete_batch_size)
		begin
			delete top (@delete_batch_size)
			from
				internal.event_message_context
			where
				operation_id = @operation_id;	

			set @rows=@@rowcount
			set @delete_row_count=@delete_row_count + @rows;
			set @delete_statement_count=@delete_statement_count + 1;
		end
	
		update
			[custom].[cleanup_logs]
		set
			[delete_row_count]=isnull([delete_row_count],0) + @delete_row_count,
			[delete_statement_count]=isnull([delete_statement_count],0) + @delete_statement_count
		where
			[cleanup_job_id]=@cleanup_job_id
			and [operation_id]=@operation_id;

		print '     cleaned ' + cast(@delete_row_count as varchar) + ' rows in ' + cast(@delete_statement_count as varchar) + ' statements from table ''event_message_context''';

		/****event_messages****/

		set @rows = @delete_batch_size;
		set @delete_row_count=0;
		set @delete_statement_count=0;

		while(@rows=@delete_batch_size)
		begin
			delete top (@delete_batch_size)
			from
				internal.event_messages
			where
				operation_id = @operation_id;	

			set @rows=@@rowcount
			set @delete_row_count=@delete_row_count + @rows;
			set @delete_statement_count=@delete_statement_count + 1;
		end
	
		update
			[custom].[cleanup_logs]
		set
			[delete_row_count]=isnull([delete_row_count],0) + @delete_row_count,
			[delete_statement_count]=isnull([delete_statement_count],0) + @delete_statement_count
		where
			[cleanup_job_id]=@cleanup_job_id
			and [operation_id]=@operation_id;

		print '     cleaned ' + cast(@delete_row_count as varchar) + ' rows in ' + cast(@delete_statement_count as varchar) + ' statements from table ''event_messages''';


		/****operation_messages ****/

		set @rows = @delete_batch_size;
		set @delete_row_count=0;
		set @delete_statement_count=0;

		while(@rows=@delete_batch_size)
		begin
			delete top (@delete_batch_size)
			from
				internal.operation_messages 
			where
				operation_id = @operation_id;	

			set @rows=@@rowcount
			set @delete_row_count=@delete_row_count + @rows;
			set @delete_statement_count=@delete_statement_count + 1;
		end
	
		update
			[custom].[cleanup_logs]
		set
			[delete_row_count]=isnull([delete_row_count],0) + @delete_row_count,
			[delete_statement_count]=isnull([delete_statement_count],0) + @delete_statement_count
		where
			[cleanup_job_id]=@cleanup_job_id
			and [operation_id]=@operation_id;

		print '     cleaned ' + cast(@delete_row_count as varchar) + ' rows in ' + cast(@delete_statement_count as varchar) + ' statements from table ''operation_messages ''';

		/****operations  ****/

		update
			internal.operations
		set
			[status]=-1
		where
			[operation_id]=@operation_id

		print '     updated status in table ''operations''';


		/**** General logging ****/
		declare @delete_timespan time(7);
		set @delete_timespan = CAST((getdate()-cast(@start_time as datetime)) as time(7));
		print 'Cleaning executed in ' + cast(@delete_timespan as varchar)

		update
			[custom].[cleanup_logs]
		set
			[state]='Successful',
			[end_time]=getdate(),
			[delete_timespan]=@delete_timespan
		where
			[cleanup_job_id]=@cleanup_job_id
			and [operation_id]=@operation_id;

		fetch next from operationCursor into @operation_id  
		set @fetch_status = @@fetch_status
	end

	close operationCursor  
	deallocate operationCursor


	print 'Updating statistics';

	set @start_time=getdate();

	exec sp_updatestats '[internal].[event_message_context]';
	exec sp_updatestats '[internal].[event_messages]';
	exec sp_updatestats '[internal].[operation_messages]';

	declare @update_statistics_timespan time(7);
	set @update_statistics_timespan = CAST((getdate()-cast(@start_time as datetime)) as time(7));
	print 'Update of statitistics executed in ' + cast(@update_statistics_timespan as varchar);

	update
		[custom].[cleanup_logs]
	set
		[update_statistics_timespan]=@update_statistics_timespan
	where
		[cleanup_job_id]=@cleanup_job_id;

	print 'Setting auto-updates to on';
	exec sp_autostats '[internal].[event_message_context]', 'On';
	exec sp_autostats '[internal].[event_messages]', 'On';
	exec sp_autostats '[internal].[operation_messages]', 'On';

end try
begin catch
	
	print 'Closing procedure because of an error';

	if (cursor_status('local', 'operationCursor') = 1
		or cursor_status('local', 'operationCursor') = 0)
	begin
		close operationCursor
		deallocate operationCursor
	end

	print 'Setting auto-updates to on';
	exec sp_autostats '[internal].[event_message_context]', 'On';
	exec sp_autostats '[internal].[event_messages]', 'On';
	exec sp_autostats '[internal].[operation_messages]', 'On';
	exec sp_autostats '[internal].[operations]', 'On';
end catch

return 0

GO


