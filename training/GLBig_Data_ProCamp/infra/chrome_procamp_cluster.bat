"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" ^
  --proxy-server="socks5://localhost:1080" ^
  --user-data-dir="%Temp%\procamp-cluster-m" http://procamp-cluster-m:8088 http://procamp-cluster-m:8081/nifi/
