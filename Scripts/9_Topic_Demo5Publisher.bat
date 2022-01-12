cd C:\Users\<username>\source\repos\BasicSend\EmitLogTopic
dotnet run "logs.critical" "a critical issue"
dotnet run "info.warn" "this will not be published"
dotnet run "logs.warn" "a warning"
echo "pausing"
timeout /t 30 /nobreak