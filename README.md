# SQLBlockingSessionsLog

*This is a sql server query to find and kill the first session that blocks another session but is not itself blocked*
*you can create job by this query and execute every 1 minute*

# Warning
*this query not killin Importan session. for example Query Running by sa or Running by SQLAGENT. you can change the filters*
*this query will kill session only have more than 2 minute execution*
