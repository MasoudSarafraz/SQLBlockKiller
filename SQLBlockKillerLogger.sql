-- بهبود یافته برای SQL Server 2008 R2 با پشتیبانی از جلسات sleeping با تراکنش باز
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

-- محاسبه بار سیستم با رویکرد ترکیبی بهینه
DECLARE @CurrentLoad INT;
DECLARE @CPU_Usage INT;
DECLARE @Active_Requests INT;
DECLARE @Blocked_Sessions INT;
DECLARE @Pending_IO INT;
DECLARE @Total_Waits BIGINT;
DECLARE @Resource_Waits BIGINT;

-- 1. دریافت استفاده از CPU (درصد)
SELECT @CPU_Usage = cntr_value
FROM sys.dm_os_performance_counters
WHERE counter_name = '% Processor Time'
      AND object_name LIKE '%Processor%'
      AND instance_name = '_Total';

-- 2. دریافت تعداد درخواست‌های فعال
SELECT @Active_Requests = COUNT(*)
FROM sys.dm_exec_requests
WHERE status = 'running';

-- 3. دریافت تعداد جلسات مسدود شده
SELECT @Blocked_Sessions = COUNT(*)
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;

-- 4. دریافت تعداد درخواست‌های I/O در انتظار
SELECT @Pending_IO = COUNT(*)
FROM sys.dm_io_pending_io_requests;

-- 5. دریافت آمار wait stats (فقط برای بررسی شدت)
SELECT @Total_Waits = SUM(wait_time_ms),
       @Resource_Waits = SUM(wait_time_ms) - SUM(signal_wait_time_ms)
FROM sys.dm_os_wait_stats;

-- محاسبه بار ترکیبی با وزن‌های بهینه
SET @CurrentLoad =
-- وزن 40% برای CPU
CASE
    WHEN @CPU_Usage > 90 THEN
        40
    WHEN @CPU_Usage > 70 THEN
        30
    WHEN @CPU_Usage > 50 THEN
        20
    WHEN @CPU_Usage > 30 THEN
        10
    ELSE
        0
END +
-- وزن 25% برای درخواست‌های فعال
CASE
    WHEN @Active_Requests > 100 THEN
        25
    WHEN @Active_Requests > 50 THEN
        15
    WHEN @Active_Requests > 20 THEN
        10
    WHEN @Active_Requests > 10 THEN
        5
    ELSE
        0
END +
-- وزن 20% برای جلسات مسدود شده
CASE
    WHEN @Blocked_Sessions > 20 THEN
        20
    WHEN @Blocked_Sessions > 10 THEN
        15
    WHEN @Blocked_Sessions > 5 THEN
        10
    WHEN @Blocked_Sessions > 2 THEN
        5
    ELSE
        0
END +
-- وزن 15% برای I/O در انتظار
CASE
    WHEN @Pending_IO > 50 THEN
        15
    WHEN @Pending_IO > 20 THEN
        10
    WHEN @Pending_IO > 10 THEN
        5
    ELSE
        0
END;

-- تنظیم نهایی بار سیستم با در نظر گرفتن wait stats
-- اگر wait stats بسیار بالا باشد، بار را افزایش می‌دهیم
IF @Resource_Waits > 1000000 -- >1000 ثانیه
    SET @CurrentLoad = @CurrentLoad + 20;
ELSE IF @Resource_Waits > 500000 -- >500 ثانیه
    SET @CurrentLoad = @CurrentLoad + 10;
ELSE IF @Resource_Waits > 100000 -- >100 ثانیه
    SET @CurrentLoad = @CurrentLoad + 5;

-- محدود کردن مقدار بین 0 تا 100
IF @CurrentLoad > 100
    SET @CurrentLoad = 100;
IF @CurrentLoad < 0
    SET @CurrentLoad = 0;

-- تنظیم آستانه پویا بر اساس بار سیستم
DECLARE @DynamicThreshold INT;
SET @DynamicThreshold = CASE
                            WHEN @CurrentLoad > 80 THEN
                                45  -- بار بسیار بالا: آستانه بسیار کم
                            WHEN @CurrentLoad > 60 THEN
                                60  -- بار بالا: آستانه کم
                            WHEN @CurrentLoad > 40 THEN
                                90  -- بار متوسط: آستانه متوسط
                            WHEN @CurrentLoad > 20 THEN
                                120 -- بار کم: آستانه بالا
                            ELSE
                                150 -- بار بسیار کم: آستانه بسیار بالا
                        END;

-- استفاده از جدول موقت برای ذخیره نتایج
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
    IsCriticalSession INT,
    SessionStatus NVARCHAR(30) -- اضافه کردن وضعیت جلسه برای دیباگ
);

-- پر کردن جدول موقت با نتایج - اصلاح شده برای پشتیبانی از جلسات sleeping
INSERT INTO #BlockingSessions
SELECT blocking.session_id,
       COUNT(DISTINCT blocked.session_id) AS BlockedCount,
       CASE
           WHEN blocking.status = 'sleeping' THEN
               'SLEEPING WITH OPEN TRANSACTION'
           ELSE
               COALESCE(blocking_text.text, 'UNKNOWN QUERY')
       END AS BlockingText,
       blocking.login_name,
       blocking.host_name,
       blocking.program_name,
       DB_NAME(blocking.database_id) AS DatabaseName,
       MIN(COALESCE(blocked.start_time, blocking.last_request_start_time)) AS StartTime,
       DATEDIFF(SECOND, MIN(COALESCE(blocked.start_time, blocking.last_request_start_time)), GETDATE()) AS DurationSeconds,
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
           ELSE
               'Unknown'
       END AS IsolationLevel,
       -- محاسبه امتیاز مسدودسازی (تعداد جلسات * زمان)
       COUNT(DISTINCT blocked.session_id)
       * DATEDIFF(SECOND, MIN(COALESCE(blocked.start_time, blocking.last_request_start_time)), GETDATE()) AS BlockingScore,
       -- تعیین سطح اقدام
       CASE
           WHEN DATEDIFF(SECOND, MIN(COALESCE(blocked.start_time, blocking.last_request_start_time)), GETDATE()) > @MaxDuration THEN
               3 -- حتماً kill
           WHEN COUNT(DISTINCT blocked.session_id) > @MinBlockedCount * 2 THEN
               2 -- kill با اولویت بالا
           WHEN
           (
               DATEDIFF(SECOND, MIN(COALESCE(blocked.start_time, blocking.last_request_start_time)), GETDATE()) > @DynamicThreshold
               AND COUNT(DISTINCT blocked.session_id) > @MinBlockedCount
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
       END AS IsCriticalSession,
       -- وضعیت جلسه برای دیباگ
       blocking.status AS SessionStatus
FROM sys.dm_exec_sessions AS blocking
    -- پیوستن به sys.dm_exec_requests برای شناسایی جلسات مسدود شده
    LEFT JOIN sys.dm_exec_requests AS blocked
        ON blocking.session_id = blocked.blocking_session_id
    -- پیوستن به sys.dm_exec_connections برای گرفتن متن کوئری
    LEFT JOIN sys.dm_exec_connections c
        ON blocking.session_id = c.session_id
    -- استفاده از OUTER APPLY برای گرفتن متن کوئری
    OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) AS blocking_text
WHERE blocking.is_user_process = 1
      -- جلسه مسدودکننده باید یا در حال اجرا باشد یا تراکنش باز داشته باشد
      AND
      (
          blocking.status = 'running'
          OR EXISTS
(
    SELECT 1
    FROM sys.dm_tran_session_transactions tst
    WHERE tst.session_id = blocking.session_id
          AND tst.is_user_transaction = 1
)
      )
      -- و باید حداقل یک جلسه مسدود شده توسط آن جلسه وجود داشته باشد
      AND EXISTS
(
    SELECT 1
    FROM sys.dm_exec_requests b
    WHERE b.blocking_session_id = blocking.session_id
)
GROUP BY blocking.session_id,
         blocking.login_name,
         blocking.host_name,
         blocking.program_name,
         blocking.database_id,
         blocking.transaction_isolation_level,
         blocking_text.text,
         blocking.status,
         blocking.last_request_start_time
HAVING DATEDIFF(SECOND, MIN(COALESCE(blocked.start_time, blocking.last_request_start_time)), GETDATE()) > 10; -- حداقل زمان برای لاگ کردن

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
    BlockingScore INT,
    SessionStatus NVARCHAR(30) -- اضافه کردن وضعیت جلسه برای دیباگ
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
       BlockingScore,
       SessionStatus
FROM #BlockingSessions
WHERE IsCriticalSession = 0
      AND ActionLevel > 0;

-- لاگ کردن تمام جلسات مسدودکننده
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
    BlockingScore INT,
    SessionStatus NVARCHAR(30) -- اضافه کردن وضعیت جلسه برای دیباگ
);

INSERT INTO #SessionsToKill
SELECT *
FROM #NonCriticalSessions
WHERE ActionLevel > 0;

-- kill کردن جلسات با اولویت‌بندی
DECLARE @KillCount INT = 0;
SELECT @KillCount = COUNT(*)
FROM #SessionsToKill;

-- برای دیباگ: نمایش تعداد جلساتی که باید kill شوند
PRINT 'Sessions to kill: ' + CAST(@KillCount AS VARCHAR(10));

WHILE @KillCount > 0 AND @MaxKillCount > 0
BEGIN
    DECLARE @CurrentSessionID INT;
    DECLARE @CurrentActionLevel INT;
    DECLARE @CurrentStatus NVARCHAR(30);
    DECLARE @KillCommand NVARCHAR(100);
    DECLARE @ErrorMessage NVARCHAR(MAX);

    -- اولویت‌بندی: ابتدا جلسات با ActionLevel بالا، سپس با BlockingScore بالا
    SELECT TOP 1
           @CurrentSessionID = BlockingSessionID,
           @CurrentActionLevel = ActionLevel,
           @CurrentStatus = SessionStatus
    FROM #SessionsToKill
    ORDER BY ActionLevel DESC,
             BlockingScore DESC;

    -- برای دیباگ: نمایش جلسه‌ای که در حال kill شدن است
    PRINT 'Attempting to kill session: ' + CAST(@CurrentSessionID AS VARCHAR(10)) + ' with ActionLevel: '
          + CAST(@CurrentActionLevel AS VARCHAR(10)) + ' Status: ' + @CurrentStatus;

    IF @CurrentSessionID IS NOT NULL
    BEGIN
        BEGIN TRY
            -- بررسی وجود جلسه بدون در نظر گرفتن وضعیت
            IF EXISTS
            (
                SELECT 1
                FROM sys.dm_exec_sessions
                WHERE session_id = @CurrentSessionID
            )
            BEGIN
                SET @KillCommand = N'KILL ' + CAST(@CurrentSessionID AS NVARCHAR(10));

                -- به‌روزرسانی لاگ قبل از kill
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

                -- برای دیباگ: نمایش دستور kill
                PRINT 'Executing: ' + @KillCommand;

                EXEC sp_executesql @KillCommand;

                -- برای دیباگ: تأیید kill موفق
                PRINT 'Session ' + CAST(@CurrentSessionID AS VARCHAR(10)) + ' killed successfully.';

                SET @MaxKillCount = @MaxKillCount - 1;
            END;
            ELSE
            BEGIN
                -- برای دیباگ: نمایش عدم وجود جلسه
                PRINT 'Session ' + CAST(@CurrentSessionID AS VARCHAR(10)) + ' no longer exists.';

                UPDATE master.dbo.BlockingSessionsLog
                SET KillReason = N'Session no longer active'
                WHERE BlockingSessionID = @CurrentSessionID
                      AND Killed = 0;
            END;
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = ERROR_MESSAGE();

            -- برای دیباگ: نمایش خطا
            PRINT 'Error killing session ' + CAST(@CurrentSessionID AS VARCHAR(10)) + ': ' + @ErrorMessage;

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

-- برای دیباگ: نمایش تعداد جلسات باقی‌مانده
PRINT 'Remaining sessions to kill: ' + CAST(@KillCount AS VARCHAR(10));

-- پاک‌سازی جداول موقت
DROP TABLE #SessionsToKill;
DROP TABLE #NonCriticalSessions;
DROP TABLE #BlockingSessions;
