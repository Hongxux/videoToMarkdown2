# verify_batch_e2e.ps1
# Batch verification script for Enterprise Services.

$videoPath = "d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
$subPath = "d:\videoToMarkdownTest2\MVP_Module2_HEANCING\tests\test_data\video_01\step2_correction_output.json"
$mergePath = "d:\videoToMarkdownTest2\MVP_Module2_HEANCING\tests\test_data\video_01\step6_merge_cross_output.json"
$timePath = "d:\videoToMarkdownTest2\MVP_Module2_HEANCING\tests\test_data\video_01\sentence_timestamps.json"
$mainTopic = "Algorithm"

$url = "http://127.0.0.1:8085/api/fusion/batch"
$url += "?path=" + [uri]::EscapeDataString($videoPath)
$url += "&subtitles_path=" + [uri]::EscapeDataString($subPath)
$url += "&merge_data_path=" + [uri]::EscapeDataString($mergePath)
$url += "&sentences_path=" + [uri]::EscapeDataString($timePath)
$url += "&main_topic=" + [uri]::EscapeDataString($mainTopic)

Write-Host "`n[BATCH E2E] Starting Full Video Batch Processing..." -ForegroundColor Cyan
Write-Host "[BATCH E2E] Target URL: $url"

try {
    # Increase timeout significantly for batch processing (e.g., 30 minutes)
    $response = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 1800
    Write-Host "`n[SUCCESS] Batch Process Completed!" -ForegroundColor Green
    $response | ForEach-Object { Write-Host "[SEGMENT RESULT] $_" -ForegroundColor Yellow }
} catch {
    Write-Host "`n[ERROR] Batch Process Failed!" -ForegroundColor Red
    Write-Host "Exception: $($_.Exception.Message)"
    exit 1
}
