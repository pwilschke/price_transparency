# run_overnight.ps1
#
# Runs run_pipeline.R while telling Windows not to sleep for the duration.
# Uses SetThreadExecutionState (a Windows API, not a persistent settings
# change) so nothing needs to be manually reverted afterward -- the "stay
# awake" request is automatically released the moment this script exits,
# whether it finishes normally, errors, or you Ctrl+C it.
#
# Deliberately requests ES_SYSTEM_REQUIRED but NOT ES_DISPLAY_REQUIRED, so
# your screen can still turn off / lock on its normal schedule to save power
# -- only the system-level sleep (which would kill the R process) is blocked.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\run_overnight.ps1
# or just right-click this file -> "Run with PowerShell"

# --- P/Invoke declaration for SetThreadExecutionState -----------------------
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class SleepBlocker {
    [FlagsAttribute]
    public enum EXECUTION_STATE : uint {
        ES_CONTINUOUS       = 0x80000000,
        ES_SYSTEM_REQUIRED  = 0x00000001
    }
    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern EXECUTION_STATE SetThreadExecutionState(EXECUTION_STATE esFlags);
}
"@

$ES_CONTINUOUS      = [SleepBlocker+EXECUTION_STATE]::ES_CONTINUOUS
$ES_SYSTEM_REQUIRED = [SleepBlocker+EXECUTION_STATE]::ES_SYSTEM_REQUIRED

# --- resolve run_pipeline.R relative to this script's own location ---------
$scriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$pipelinePath = Join-Path $scriptDir "run_pipeline.R"

if (-not (Test-Path $pipelinePath)) {
    Write-Error "Could not find run_pipeline.R next to this script at: $pipelinePath"
    exit 1
}

Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Blocking sleep and starting pipeline..."
[SleepBlocker]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED) | Out-Null

try {
    Rscript $pipelinePath
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Pipeline finished with exit code $LASTEXITCODE."
}
finally {
    # Always release the sleep-block, even if Rscript errored or was interrupted.
    [SleepBlocker]::SetThreadExecutionState($ES_CONTINUOUS) | Out-Null
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Sleep block released."
}
