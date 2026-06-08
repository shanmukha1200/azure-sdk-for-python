# Drive the supervised test app: show readiness across a crash and a hang.
$Base = if ($env:BASE) { $env:BASE } else { "http://localhost:8088" }

function Code($path) {
    try {
        (Invoke-WebRequest -Uri "$Base$path" -Method Get -UseBasicParsing -TimeoutSec 10).StatusCode
    } catch {
        if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
    }
}

Write-Host "== baseline =="
Write-Host "whoami:    $((Invoke-WebRequest "$Base/control/whoami" -UseBasicParsing).Content)"
Write-Host "readiness: $(Code '/readiness')  (expect 200)"

Write-Host "`n== crash (worker dies, supervisor restarts) =="
try { Invoke-WebRequest "$Base/control/crash" -UseBasicParsing -TimeoutSec 5 | Out-Null } catch {}
for ($i = 1; $i -le 20; $i++) {
    Write-Host ("  t+{0}x0.25s  readiness={1}" -f $i, (Code '/readiness'))
    Start-Sleep -Milliseconds 250
}
Write-Host "whoami:    $((Invoke-WebRequest "$Base/control/whoami" -UseBasicParsing).Content)  (expect NEW pid)"

Write-Host "`n== hang (worker alive but unresponsive for 8s) =="
Start-Job -ScriptBlock { param($b) try { Invoke-WebRequest "$b/control/hang?seconds=8" -UseBasicParsing -TimeoutSec 30 } catch {} } -ArgumentList $Base | Out-Null
Start-Sleep -Seconds 1
for ($i = 1; $i -le 10; $i++) {
    Write-Host ("  t+{0}s  readiness={1}  (expect 424 while frozen)" -f $i, (Code '/readiness'))
    Start-Sleep -Seconds 1
}
Write-Host "readiness: $(Code '/readiness')  (expect 200 again)"
