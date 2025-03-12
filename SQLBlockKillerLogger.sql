IF NOT EXISTS (
		SELECT 1
		FROM master.dbo.sysobjects
		WHERE NAME = 'BlockingSessionsLog'
			AND xtype = 'U'
		)
	CREATE TABLE master.dbo.BlockingSessionsLog (
		LogID INT IDENTITY(1, 1) PRIMARY KEY
		,BlockingSessionID INT
		,BlockedSessionCount INT
		,BlockingQueryText NVARCHAR(MAX)
		,BlockingLoginName NVARCHAR(128)
		,BlockingHostName NVARCHAR(128)
		,BlockingProgramName NVARCHAR(128)
		,BlockingDatabaseName NVARCHAR(128)
		,BlockingStartTime DATETIME
		,BlockingDurationSeconds INT
		,TransactionIsolationLevel NVARCHAR(50)
		,LogTime DATETIME DEFAULT GETDATE()
		,Killed BIT DEFAULT 0
		,KillReason NVARCHAR(255)
		,ErrorMessage NVARCHAR(MAX)
		);

WITH BlockingSessions
AS (
	SELECT blocking.session_id
		,COUNT(blocked.session_id) AS BlockedCount
		,blocking_text.TEXT AS BlockingText
		,blocking.login_name
		,blocking.host_name
		,blocking.program_name
		,DB_NAME(blocking.database_id) AS DatabaseName
		,MIN(blocked.start_time) AS StartTime
		,DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) AS DurationSeconds
		,CASE blocking.transaction_isolation_level
			WHEN 0
				THEN N'Unspecified'
			WHEN 1
				THEN N'ReadUncommitted'
			WHEN 2
				THEN N'ReadCommitted'
			WHEN 3
				THEN N'Repeatable'
			WHEN 4
				THEN N'Serializable'
			WHEN 5
				THEN N'Snapshot'
			END AS IsolationLevel
		,CASE 
			WHEN blocking.program_name LIKE N'%SQLAgent%'
				OR blocking.login_name = N'sa'
				OR blocking.program_name IN (
					N'SQLAgent - TSQL JobStep'
					,N'Replication Snapshot Agent'
					,N'Replication Log Reader Agent'
					,N'Replication Distribution Agent'
					,N'Replication Merge Agent'
					,N'SSIS-Package'
					)
				OR EXISTS (
					SELECT 1
					FROM sys.dm_exec_connections c
					CROSS APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) st
					WHERE c.session_id = blocking.session_id
						AND (
							st.TEXT LIKE N'%BACKUP DATABASE%'
							OR st.TEXT LIKE N'%RESTORE DATABASE%'
							OR st.TEXT LIKE N'%sp_start_backup%'
							OR st.TEXT LIKE N'%sp_configure%'
							)
					)
				OR DB_NAME(blocking.database_id) IN (
					N'master'
					,N'msdb'
					,N'model'
					,N'SSISDB'
					,N'Distribution'
					)
				THEN 1
			ELSE 0
			END AS IsCriticalSession
	FROM sys.dm_exec_sessions AS blocking
	INNER JOIN sys.dm_exec_requests AS blocked ON blocking.session_id = blocked.blocking_session_id
	CROSS APPLY (
		SELECT TEXT
		FROM sys.dm_exec_connections c
		CROSS APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle)
		WHERE c.session_id = blocking.session_id
		) AS blocking_text(TEXT)
	WHERE blocked.blocking_session_id IS NOT NULL
		AND blocked.session_id <> blocked.blocking_session_id
	GROUP BY blocking.session_id
		,blocking.login_name
		,blocking.host_name
		,blocking.program_name
		,blocking.database_id
		,blocking.transaction_isolation_level
		,blocking_text.TEXT
	HAVING DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) > 120
	)
SELECT BlockingSessionID = session_id
	,BlockedCount
	,BlockingQueryText = BlockingText
	,BlockingLoginName = login_name
	,BlockingHostName = host_name
	,BlockingProgramName = program_name
	,BlockingDatabaseName = DatabaseName
	,BlockingStartTime = StartTime
	,BlockingDurationSeconds = DurationSeconds
	,TransactionIsolationLevel = IsolationLevel
	,IsCriticalSession
INTO #TempBlockingSessions
FROM BlockingSessions
WHERE IsCriticalSession = 0;

DECLARE @BlockingSessionID INT
	,@KillCommand NVARCHAR(100)
	,@ErrorMessage NVARCHAR(MAX);

DECLARE SESSION_CURSOR CURSOR LOCAL FAST_FORWARD
FOR
SELECT BlockingSessionID
FROM #TempBlockingSessions;

OPEN SESSION_CURSOR;

FETCH NEXT
FROM SESSION_CURSOR
INTO @BlockingSessionID;

WHILE @@FETCH_STATUS = 0
BEGIN
	BEGIN TRY
		IF EXISTS (
				SELECT 1
				FROM sys.dm_exec_sessions
				WHERE session_id = @BlockingSessionID
				)
		BEGIN
			SET @KillCommand = N'KILL ' + CAST(@BlockingSessionID AS NVARCHAR(10));

			INSERT INTO master.dbo.BlockingSessionsLog (
				BlockingSessionID
				,BlockedSessionCount
				,BlockingQueryText
				,BlockingLoginName
				,BlockingHostName
				,BlockingProgramName
				,BlockingDatabaseName
				,BlockingStartTime
				,BlockingDurationSeconds
				,TransactionIsolationLevel
				,KillReason
				,Killed
				)
			SELECT BlockingSessionID
				,BlockedCount
				,BlockingQueryText
				,BlockingLoginName
				,BlockingHostName
				,BlockingProgramName
				,BlockingDatabaseName
				,BlockingStartTime
				,BlockingDurationSeconds
				,TransactionIsolationLevel
				,N'Blocking Other Sessions - Killed by Automated Process'
				,1
			FROM #TempBlockingSessions
			WHERE BlockingSessionID = @BlockingSessionID;

			EXEC sp_executesql @KillCommand;
		END
	END TRY

	BEGIN CATCH
		SET @ErrorMessage = ERROR_MESSAGE();

		INSERT INTO master.dbo.BlockingSessionsLog (
			BlockingSessionID
			,BlockedSessionCount
			,BlockingQueryText
			,BlockingLoginName
			,BlockingHostName
			,BlockingProgramName
			,BlockingDatabaseName
			,BlockingStartTime
			,BlockingDurationSeconds
			,TransactionIsolationLevel
			,KillReason
			,Killed
			,ErrorMessage
			)
		SELECT BlockingSessionID
			,BlockedCount
			,BlockingQueryText
			,BlockingLoginName
			,BlockingHostName
			,BlockingProgramName
			,BlockingDatabaseName
			,BlockingStartTime
			,BlockingDurationSeconds
			,TransactionIsolationLevel
			,N'Error attempting to kill session'
			,0
			,@ErrorMessage
		FROM #TempBlockingSessions
		WHERE BlockingSessionID = @BlockingSessionID;
	END CATCH

	FETCH NEXT
	FROM SESSION_CURSOR
	INTO @BlockingSessionID;
END

CLOSE SESSION_CURSOR;

DEALLOCATE SESSION_CURSOR;

DROP TABLE #TempBlockingSessions;
