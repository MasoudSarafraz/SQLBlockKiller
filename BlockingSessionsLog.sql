-- ????? ???? ??? ??????? (??? ???? ?????)
IF NOT EXISTS (
                  SELECT
                      *
                  FROM master.sys.tables
                  WHERE
                      name = 'BlockingSessionsLog'
              )
    CREATE TABLE master.dbo.BlockingSessionsLog
        (
            LogID                      INT           IDENTITY (1, 1) PRIMARY KEY
            ,BlockingSessionID         INT
            ,BlockedSessionCount       INT
            ,BlockingQueryText         NVARCHAR (MAX)
            ,BlockingLoginName         NVARCHAR (128)
            ,BlockingHostName          NVARCHAR (128)
            ,BlockingProgramName       NVARCHAR (128)
            ,BlockingDatabaseName      NVARCHAR (128)
            ,BlockingStartTime         DATETIME
            ,BlockingDurationSeconds   INT
            ,TransactionIsolationLevel NVARCHAR (50)
            ,LogTime                   DATETIME
                 DEFAULT GETDATE ()
            ,Killed                    BIT
                 DEFAULT 0
            ,KillReason                NVARCHAR (255)
            ,ErrorMessage              NVARCHAR (MAX)
        );

-- ??????? ??????? Blocking ? ????? ?? ???? ????
DECLARE @BlockingSessions TABLE
    (
        RowID                      INT IDENTITY (1, 1) PRIMARY KEY
        ,BlockingSessionID         INT
        ,BlockedSessionCount       INT
        ,BlockingQueryText         NVARCHAR (MAX)
        ,BlockingLoginName         NVARCHAR (128)
        ,BlockingHostName          NVARCHAR (128)
        ,BlockingProgramName       NVARCHAR (128)
        ,BlockingDatabaseName      NVARCHAR (128)
        ,BlockingStartTime         DATETIME
        ,BlockingDurationSeconds   INT
        ,TransactionIsolationLevel NVARCHAR (50)
        ,IsCriticalSession         BIT
    );

-- ???????? ???????? ??????? Blocking
INSERT INTO @BlockingSessions
(
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
    ,IsCriticalSession
)
            SELECT
                blocking.session_id
                ,COUNT (blocked.session_id)
                ,sql_text.text
                ,blocking.login_name
                ,blocking.host_name
                ,blocking.program_name
                ,DB_NAME (blocking.database_id)
                ,MIN (blocked.start_time)
                ,DATEDIFF (SECOND, MIN (blocked.start_time), GETDATE ())
                ,CASE blocking.transaction_isolation_level
                     WHEN 0 THEN 'Unspecified'
                     WHEN 1 THEN 'ReadUncommitted'
                     WHEN 2 THEN 'ReadCommitted'
                     WHEN 3 THEN 'Repeatable'
                     WHEN 4 THEN 'Serializable'
                     WHEN 5 THEN 'Snapshot'
                 END
                ,CASE WHEN blocking.program_name LIKE '%SQLAgent%' THEN 1
                     WHEN blocking.login_name = 'sa' THEN 1
                     WHEN sql_text.text LIKE '%BACKUP%DATABASE%' THEN 1
                     WHEN DB_NAME (blocking.database_id) IN
                     (
                         'master'
                         ,'msdb'
                         ,'model'
                     ) THEN 1
                 ELSE 0
                 END
            FROM sys.dm_exec_sessions                                 AS blocking
                INNER JOIN sys.dm_exec_requests                       AS blocked ON blocking.session_id = blocked.blocking_session_id
                CROSS APPLY sys.dm_exec_sql_text (blocked.sql_handle) AS sql_text -- ????? ?? blocked.sql_handle
            WHERE
                blocked.blocking_session_id IS NOT NULL
                AND blocked.session_id <> blocked.blocking_session_id
            GROUP BY
                blocking.session_id
                ,blocking.login_name
                ,blocking.host_name
                ,blocking.program_name
                ,blocking.database_id
                ,blocking.transaction_isolation_level
                ,sql_text.text
            HAVING
                DATEDIFF (SECOND, MIN (blocked.start_time), GETDATE ()) > 120 -- ??? ?? ? ?????
                AND CASE WHEN blocking.program_name LIKE '%SQLAgent%' THEN 1
                        WHEN blocking.login_name = 'sa' THEN 1
                        WHEN sql_text.text LIKE '%BACKUP%DATABASE%' THEN 1
                        WHEN DB_NAME (blocking.database_id) IN
                        (
                            'master'
                            ,'msdb'
                            ,'model'
                        ) THEN 1
                    ELSE 0
                    END                                                 = 0;

-- ?????? ?????? ?? ???? WHILE
DECLARE
    @RowID                      INT
    ,@BlockingSessionID         INT
    ,@BlockedSessionCount       INT
    ,@BlockingQueryText         NVARCHAR (MAX)
    ,@BlockingLoginName         NVARCHAR (128)
    ,@BlockingHostName          NVARCHAR (128)
    ,@BlockingProgramName       NVARCHAR (128)
    ,@BlockingDatabaseName      NVARCHAR (128)
    ,@BlockingStartTime         DATETIME
    ,@BlockingDurationSeconds   INT
    ,@TransactionIsolationLevel NVARCHAR (50)
    ,@KillReason                NVARCHAR (255)
    ,@KillCommand               NVARCHAR (MAX)
    ,@Error                     NVARCHAR (MAX);

WHILE EXISTS (
                 SELECT
                     1
                 FROM @BlockingSessions
             )
    BEGIN
        SELECT TOP 1
               @RowID                      = RowID
               ,@BlockingSessionID         = BlockingSessionID
               ,@BlockedSessionCount       = BlockedSessionCount
               ,@BlockingQueryText         = BlockingQueryText
               ,@BlockingLoginName         = BlockingLoginName
               ,@BlockingHostName          = BlockingHostName
               ,@BlockingProgramName       = BlockingProgramName
               ,@BlockingDatabaseName      = BlockingDatabaseName
               ,@BlockingStartTime         = BlockingStartTime
               ,@BlockingDurationSeconds   = BlockingDurationSeconds
               ,@TransactionIsolationLevel = TransactionIsolationLevel
        FROM @BlockingSessions
        ORDER BY
            RowID;

        BEGIN TRY
            IF EXISTS (
                          SELECT
                              1
                          FROM sys.dm_exec_sessions
                          WHERE
                              session_id       = @BlockingSessionID
                              AND program_name = @BlockingProgramName
                              AND login_name   = @BlockingLoginName
                      )
                BEGIN
                    SET @KillReason = N'Blocking ??? ?? ? ????? ?? ??????? ' + @BlockingDatabaseName;
                    SET @KillCommand = N'KILL ' + CAST (@BlockingSessionID AS NVARCHAR (10));
                    EXEC sp_executesql @KillCommand;

                    INSERT INTO master.dbo.BlockingSessionsLog
                    (
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
                    VALUES
                    (
                        @BlockingSessionID
                        ,@BlockedSessionCount
                        ,@BlockingQueryText
                        ,@BlockingLoginName
                        ,@BlockingHostName
                        ,@BlockingProgramName
                        ,@BlockingDatabaseName
                        ,@BlockingStartTime
                        ,@BlockingDurationSeconds
                        ,@TransactionIsolationLevel
                        ,@KillReason
                        ,1
                    );

                --PRINT '??? ' + CAST(@BlockingSessionID AS NVARCHAR(10)) + ' Kill ??.';
                END
        END TRY
        BEGIN CATCH
            SET @Error = ERROR_MESSAGE ();
            INSERT INTO master.dbo.BlockingSessionsLog
            (
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
            VALUES
            (
                @BlockingSessionID
                ,@BlockedSessionCount
                ,@BlockingQueryText
                ,@BlockingLoginName
                ,@BlockingHostName
                ,@BlockingProgramName
                ,@BlockingDatabaseName
                ,@BlockingStartTime
                ,@BlockingDurationSeconds
                ,@TransactionIsolationLevel
                ,@KillReason
                ,0
                ,@Error
            );
        END CATCH

        DELETE FROM @BlockingSessions
        WHERE
            RowID = @RowID;
    END


SELECT * FROM master.dbo.BlockingSessionsLog WHERE LogTime >= DATEADD(MINUTE, -5, GETDATE());