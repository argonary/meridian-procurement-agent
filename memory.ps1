$divider = "========================================"

Write-Host $divider
Write-Host "  GIT BRANCH"
Write-Host $divider
git rev-parse --abbrev-ref HEAD

Write-Host ""
Write-Host $divider
Write-Host "  LAST 5 COMMITS"
Write-Host $divider
git log --format="%ad  %h  %s" --date=short -5

Write-Host ""
Write-Host $divider
Write-Host "  WORKING TREE STATUS"
Write-Host $divider
$status = git status --short
if ($status) {
    Write-Host $status
} else {
    Write-Host "(clean - no modified or untracked files)"
}

Write-Host ""
Write-Host $divider
Write-Host "  primer.md"
Write-Host $divider
Get-Content primer.md
