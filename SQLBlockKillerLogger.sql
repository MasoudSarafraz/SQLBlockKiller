IF NOT EXISTS
(
    SELECT 1
    FROM master.dbo.sysobjects
    WHERE name = 'BlockingSessionsLog'
          AND xtype = 'U'
)
    CREATE TABLE master.dbo.BlockingSessionsLog
    (
        LogID INT IDENTITY(1, 1) PRIMARY KEY,
        BlockingSessionID INT,
        BlockedSessionCount INT,
        BlockingQueryText NVARCHAR(MAX),
        BlockingLoginName NVARCHAR(128),
        BlockingHostName NVARCHAR(128),
        BlockingProgramName NVARCHAR(128),
        BlockingDatabaseName NVARCHAR(128),
        BlockingStartTime DATETIME,
        BlockingDurationSeconds INT,
        TransactionIsolationLevel NVARCHAR(50),
        LogTime DATETIME
            DEFAULT GETDATE(),
        Killed BIT
            DEFAULT 0,
        KillReason NVARCHAR(255),
        ErrorMessage NVARCHAR(MAX)
    );

-- تعریف آستانه‌های پویا
DECLARE @MinBlockedCount INT = 5; -- حداقل تعداد جلسات مسدود شده برای kill
DECLARE @MinDuration INT = 30; -- حداقل زمان مسدودسازی برای kill (ثانیه)
DECLARE @MaxDuration INT = 300; -- حداکثر زمان مجاز (ثانیه)
DECLARE @MaxKillCount INT = 10; -- حداکثر تعداد kill در هر اجرا

-- محاسبه بار فعلی سیستم برای تنظیم آستانه
DECLARE @CurrentLoad INT;
SELECT @CurrentLoad = COUNT(*)
FROM sys.dm_exec_sessions
WHERE status = 'running';

DECLARE @DynamicThreshold INT;
SET @DynamicThreshold = CASE
                            WHEN @CurrentLoad > 100 THEN
                                60  -- بار بالا: آستانه کمتر
                            WHEN @CurrentLoad > 50 THEN
                                90  -- بار متوسط
                            ELSE
                                120 -- بار کم: آستانه بیشتر
                        END;

-- استفاده از جدول موقت برای ذخیره نتایج CTE
IF OBJECT_ID('tempdb..#BlockingSessions') IS NOT NULL
    DROP TABLE #BlockingSessions;

CREATE TABLE #BlockingSessions
(
    session_id INT,
    BlockedCount INT,
    BlockingText NVARCHAR(MAX),
    login_name NVARCHAR(128),
    host_name NVARCHAR(128),
    program_name NVARCHAR(128),
    DatabaseName NVARCHAR(128),
    StartTime DATETIME,
    DurationSeconds INT,
    IsolationLevel NVARCHAR(50),
    BlockingScore INT,
    ActionLevel INT,
    IsCriticalSession INT
);

-- پر کردن جدول موقت با نتایج CTE
INSERT INTO #BlockingSessions
SELECT blocking.session_id,
       COUNT(blocked.session_id) AS BlockedCount,
       blocking_text.text AS BlockingText,
       blocking.login_name,
       blocking.host_name,
       blocking.program_name,
       DB_NAME(blocking.database_id) AS DatabaseName,
       MIN(blocked.start_time) AS StartTime,
       DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) AS DurationSeconds,
       CASE blocking.transaction_isolation_level
           WHEN 0 THEN
               N'Unspecified'
           WHEN 1 THEN
               N'ReadUncommitted'
           WHEN 2 THEN
               N'ReadCommitted'
           WHEN 3 THEN
               N'Repeatable'
           WHEN 4 THEN
               N'Serializable'
           WHEN 5 THEN
               N'Snapshot'
       END AS IsolationLevel,
       -- محاسبه امتیاز مسدودسازی
       COUNT(blocked.session_id) * DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) AS BlockingScore,
       -- تعیین سطح اقدام
       CASE
           WHEN DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) > @MaxDuration THEN
               3 -- حتماً kill
           WHEN COUNT(blocked.session_id) > @MinBlockedCount * 2 THEN
               2 -- kill با اولویت بالا
           WHEN
           (
               DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) > @DynamicThreshold
               AND COUNT(blocked.session_id) > @MinBlockedCount
           ) THEN
               1 -- kill با اولویت پایین
           ELSE
               0 -- فقط لاگ کن
       END AS ActionLevel,
       -- تشخیص حیاتی بودن
       CASE
           WHEN blocking.program_name LIKE N'%SQLAgent%' THEN
               1
           WHEN blocking.login_name = N'sa' THEN
               1
           WHEN blocking.program_name IN ( N'SQLAgent - TSQL JobStep', N'Replication Snapshot Agent',
                                           N'Replication Log Reader Agent', N'Replication Distribution Agent',
                                           N'Replication Merge Agent', N'SSIS-Package'
                                         ) THEN
               1
           WHEN DB_NAME(blocking.database_id) IN ( N'master', N'msdb', N'model', N'SSISDB', N'Distribution' ) THEN
               1
           WHEN blocking_text.text LIKE N'%BACKUP DATABASE%' THEN
               1
           WHEN blocking_text.text LIKE N'%RESTORE DATABASE%' THEN
               1
           WHEN blocking_text.text LIKE N'%sp_start_backup%' THEN
               1
           WHEN blocking_text.text LIKE N'%sp_configure%' THEN
               1
           ELSE
               0
       END AS IsCriticalSession
FROM sys.dm_exec_sessions AS blocking
    INNER JOIN sys.dm_exec_requests AS blocked
        ON blocking.session_id = blocked.blocking_session_id
    LEFT JOIN sys.dm_exec_connections c
        ON blocking.session_id = c.session_id
    CROSS APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) AS blocking_text
WHERE blocked.blocking_session_id IS NOT NULL
      AND blocked.session_id <> blocked.blocking_session_id
      AND blocking.is_user_process = 1
GROUP BY blocking.session_id,
         blocking.login_name,
         blocking.host_name,
         blocking.program_name,
         blocking.database_id,
         blocking.transaction_isolation_level,
         blocking_text.text
HAVING DATEDIFF(SECOND, MIN(blocked.start_time), GETDATE()) > 10; -- حداقل زمان برای لاگ کردن

-- ایجاد جدول موقت برای جلسات غیرحیاتی
IF OBJECT_ID('tempdb..#NonCriticalSessions') IS NOT NULL
    DROP TABLE #NonCriticalSessions;

CREATE TABLE #NonCriticalSessions
(
    BlockingSessionID INT PRIMARY KEY,
    BlockedCount INT,
    BlockingQueryText NVARCHAR(MAX),
    BlockingLoginName NVARCHAR(128),
    BlockingHostName NVARCHAR(128),
    BlockingProgramName NVARCHAR(128),
    BlockingDatabaseName NVARCHAR(128),
    BlockingStartTime DATETIME,
    BlockingDurationSeconds INT,
    TransactionIsolationLevel NVARCHAR(50),
    ActionLevel INT,
    BlockingScore INT
);

-- پر کردن جدول جلسات غیرحیاتی
INSERT INTO #NonCriticalSessions
SELECT session_id AS BlockingSessionID,
       BlockedCount,
       BlockingText AS BlockingQueryText,
       login_name AS BlockingLoginName,
       host_name AS BlockingHostName,
       program_name AS BlockingProgramName,
       DatabaseName AS BlockingDatabaseName,
       StartTime AS BlockingStartTime,
       DurationSeconds AS BlockingDurationSeconds,
       IsolationLevel AS TransactionIsolationLevel,
       ActionLevel,
       BlockingScore
FROM #BlockingSessions
WHERE IsCriticalSession = 0
      AND ActionLevel > 0;

-- لاگ کردن تمام جلسات مسدودکننده (حتی اگر kill نشوند)
INSERT INTO master.dbo.BlockingSessionsLog
(
    BlockingSessionID,
    BlockedSessionCount,
    BlockingQueryText,
    BlockingLoginName,
    BlockingHostName,
    BlockingProgramName,
    BlockingDatabaseName,
    BlockingStartTime,
    BlockingDurationSeconds,
    TransactionIsolationLevel,
    KillReason,
    Killed
)
SELECT BlockingSessionID,
       BlockedCount,
       BlockingQueryText,
       BlockingLoginName,
       BlockingHostName,
       BlockingProgramName,
       BlockingDatabaseName,
       BlockingStartTime,
       BlockingDurationSeconds,
       TransactionIsolationLevel,
       CASE ActionLevel
           WHEN 3 THEN
               N'Critical blocking - Will be killed'
           WHEN 2 THEN
               N'High priority blocking - Will be killed'
           WHEN 1 THEN
               N'Low priority blocking - Will be killed'
           ELSE
               N'Monitored blocking'
       END,
       0
FROM #NonCriticalSessions;

-- فقط جلساتی که باید kill شوند را انتخاب کن
IF OBJECT_ID('tempdb..#SessionsToKill') IS NOT NULL
    DROP TABLE #SessionsToKill;

CREATE TABLE #SessionsToKill
(
    BlockingSessionID INT PRIMARY KEY,
    BlockedCount INT,
    BlockingQueryText NVARCHAR(MAX),
    BlockingLoginName NVARCHAR(128),
    BlockingHostName NVARCHAR(128),
    BlockingProgramName NVARCHAR(128),
    BlockingDatabaseName NVARCHAR(128),
    BlockingStartTime DATETIME,
    BlockingDurationSeconds INT,
    TransactionIsolationLevel NVARCHAR(50),
    ActionLevel INT,
    BlockingScore INT
);

INSERT INTO #SessionsToKill
SELECT *
FROM #NonCriticalSessions
WHERE ActionLevel > 0;

-- kill کردن جلسات با اولویت‌بندی
DECLARE @KillCount INT = 0;
SELECT @KillCount = COUNT(*)
FROM #SessionsToKill;

WHILE @KillCount > 0 AND @MaxKillCount > 0
BEGIN
    DECLARE @CurrentSessionID INT;
    DECLARE @CurrentActionLevel INT;
    DECLARE @KillCommand NVARCHAR(100);
    DECLARE @ErrorMessage NVARCHAR(MAX);

    -- اولویت‌بندی: ابتدا جلسات با ActionLevel بالا، سپس با BlockingScore بالا
    SELECT TOP 1
           @CurrentSessionID = BlockingSessionID,
           @CurrentActionLevel = ActionLevel
    FROM #SessionsToKill
    ORDER BY ActionLevel DESC,
             BlockingScore DESC;

    IF @CurrentSessionID IS NOT NULL
    BEGIN
        BEGIN TRY
            IF EXISTS
            (
                SELECT 1
                FROM sys.dm_exec_sessions
                WHERE session_id = @CurrentSessionID
                      AND status = 'running'
            )
            BEGIN
                SET @KillCommand = N'KILL ' + CAST(@CurrentSessionID AS NVARCHAR(10));

                -- به‌روزرسانی لاگ
                UPDATE master.dbo.BlockingSessionsLog
                SET KillReason = CASE @CurrentActionLevel
                                     WHEN 3 THEN
                                         N'Critical blocking - Killed by Automated Process'
                                     WHEN 2 THEN
                                         N'High priority blocking - Killed by Automated Process'
                                     WHEN 1 THEN
                                         N'Low priority blocking - Killed by Automated Process'
                                 END,
                    Killed = 1
                WHERE BlockingSessionID = @CurrentSessionID
                      AND Killed = 0;

                EXEC sp_executesql @KillCommand;
                SET @MaxKillCount = @MaxKillCount - 1;
            END;
            ELSE
            BEGIN
                UPDATE master.dbo.BlockingSessionsLog
                SET KillReason = N'Session no longer active'
                WHERE BlockingSessionID = @CurrentSessionID
                      AND Killed = 0;
            END;
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = ERROR_MESSAGE();

            UPDATE master.dbo.BlockingSessionsLog
            SET KillReason = N'Error attempting to kill session',
                ErrorMessage = @ErrorMessage
            WHERE BlockingSessionID = @CurrentSessionID
                  AND Killed = 0;
        END CATCH;

        DELETE FROM #SessionsToKill
        WHERE BlockingSessionID = @CurrentSessionID;
    END;

    SELECT @KillCount = COUNT(*)
    FROM #SessionsToKill;
END;

-- پاک‌سازی جداول موقت
DROP TABLE #SessionsToKill;
DROP TABLE #NonCriticalSessions;
DROP TABLE #BlockingSessions;
