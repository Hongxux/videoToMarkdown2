$url = "http://localhost:8085/api/fusion/batch"
Write-Host "Base URL: $url"

$videoPath = "d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
$jsonBaseDir = "d:\videoToMarkdownTest2\MVP_Module2_HEANCING\tests\test_data\video_01"

Add-Type -AssemblyName System.Web

$path = [System.Web.HttpUtility]::UrlEncode($videoPath)
$sentences = [System.Web.HttpUtility]::UrlEncode("$jsonBaseDir\sentence_timestamps.json")
$subtitles = [System.Web.HttpUtility]::UrlEncode("$jsonBaseDir\step2_correction_output.json")
$merge = [System.Web.HttpUtility]::UrlEncode("$jsonBaseDir\step6_merge_cross_output.json")
$topic = "test_topic"

$fullUrl = "$url`?path=$path&sentences_path=$sentences&subtitles_path=$subtitles&merge_data_path=$merge&main_topic=$topic"

Write-Host "Calling URL: $fullUrl"

try {
    $response = Invoke-RestMethod -Uri $fullUrl -Method Get -TimeoutSec 1800
    Write-Host "Response received:"
    $response | ConvertTo-Json -Depth 10 | Write-Host
} catch {
    Write-Host "Error: $($_.Exception.Message)"
    if ($_.Exception.Response) {
        Write-Host "Status: $($_.Exception.Response.StatusCode)"
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        Write-Host "Body: $($reader.ReadToEnd())"
    }
}
