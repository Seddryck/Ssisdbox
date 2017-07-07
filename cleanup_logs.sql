USE [SSISDB]
GO

/****** Object:  Table [custom].[cleanup_logs]    Script Date: 7/07/2017 12:58:54 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [custom].[cleanup_logs](
	[cleanup_log_id] [bigint] IDENTITY(1,1) NOT NULL,
	[object_id] [bigint] NOT NULL,
	[project_name] [sysname] NOT NULL,
	[operation_id] [bigint] NOT NULL,
	[cleanup_job_id] [bigint] NOT NULL,
	[cleanup_job_start_time] [datetime2](7) NOT NULL,
	[state] [varchar](20) NOT NULL,
	[start_time] [datetime2](7) NULL,
	[end_time] [datetime2](7) NULL,
	[delete_row_count] [bigint] NULL,
	[delete_statement_count] [bigint] NULL,
	[delete_timespan] [time](7) NULL,
	[update_statistics_timespan] [time](7) NULL,
 CONSTRAINT [PK_cleanup_logs] PRIMARY KEY CLUSTERED 
(
	[cleanup_log_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING ON
GO


