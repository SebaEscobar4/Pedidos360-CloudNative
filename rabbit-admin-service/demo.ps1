# Demostracion de la API de rabbit-admin-service.
# Uso:  .\demo.ps1            (pide el Access Token de un usuario con rol ADMIN; lo escrito no se muestra)
#       .\demo.ps1 -BaseUrl http://otro-host:8081
param([string]$BaseUrl = "http://localhost:8081")

$seguro = Read-Host "Pega el Access Token (sin la palabra Bearer)" -AsSecureString
$token = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($seguro)).Trim()

function Probar([string]$Titulo, [string]$Metodo, [string]$Ruta, [string]$Cuerpo, [bool]$ConToken = $true) {
    $argumentos = @("-s", "-o", "$env:TEMP\rabbit-admin-demo.json", "-w", "%{http_code}", "-X", $Metodo, "$BaseUrl$Ruta")
    if ($ConToken) { $argumentos += @("-H", "Authorization: Bearer $token") }
    if ($Cuerpo) {
        # Se envia desde un archivo para que PowerShell no altere las comillas del JSON
        Set-Content -Path "$env:TEMP\rabbit-admin-body.json" -Value $Cuerpo -Encoding ascii
        $argumentos += @("-H", "Content-Type: application/json", "--data-binary", "@$env:TEMP\rabbit-admin-body.json")
    }
    $codigo = & curl.exe @argumentos
    $respuesta = Get-Content "$env:TEMP\rabbit-admin-demo.json" -Raw -ErrorAction SilentlyContinue
    Write-Host ""
    Write-Host "$Titulo" -ForegroundColor Cyan
    Write-Host "  $Metodo $Ruta  ->  HTTP $codigo"
    if ($respuesta) { Write-Host "  $($respuesta.Trim())" -ForegroundColor DarkGray }
}

Write-Host "===== CREACION =====" -ForegroundColor Yellow
Probar "1. Crear exchange" "POST" "/api/exchanges" '{"name":"pedidos360.reportes.exchange","type":"topic"}'
Probar "2. Crear cola con DLQ, TTL y largo maximo" "POST" "/api/queues" '{"name":"pedidos360.reportes.queue","deadLetterExchange":"pedidos360.dlx","deadLetterRoutingKey":"reportes.dlq","messageTtl":60000,"maxLength":1000}'
Probar "3. Crear binding" "POST" "/api/bindings" '{"queue":"pedidos360.reportes.queue","exchange":"pedidos360.reportes.exchange","routingKey":"reporte.#"}'
Probar "4. Consultar cola" "GET" "/api/queues/pedidos360.reportes.queue" ""

Write-Host ""
Write-Host "===== VALIDACION Y ERRORES =====" -ForegroundColor Yellow
Probar "5. Nombre vacio (400)" "POST" "/api/queues" '{"name":""}'
Probar "6. Configuracion invalida: nombre reservado, TTL negativo, DLQ incompleta (400)" "POST" "/api/queues" '{"name":"amq.demo","messageTtl":-5,"deadLetterRoutingKey":"x"}'
Probar "7. Tipo de exchange invalido (400)" "POST" "/api/exchanges" '{"name":"demo.exchange","type":"inventado"}'
Probar "8. Cola duplicada (409)" "POST" "/api/queues" '{"name":"pedidos360.reportes.queue"}'
Probar "9. Cola inexistente (404)" "DELETE" "/api/queues/no.existe" ""
Probar "10. Sin token (401)" "POST" "/api/queues" '{"name":"demo.queue"}' $false

Write-Host ""
Write-Host "===== ELIMINACION =====" -ForegroundColor Yellow
Probar "11. Eliminar binding" "DELETE" "/api/bindings?queue=pedidos360.reportes.queue&exchange=pedidos360.reportes.exchange&routingKey=reporte.%23" ""
Probar "12. Eliminar cola" "DELETE" "/api/queues/pedidos360.reportes.queue" ""
Probar "13. Eliminar exchange" "DELETE" "/api/exchanges/pedidos360.reportes.exchange" ""

Remove-Item "$env:TEMP\rabbit-admin-demo.json", "$env:TEMP\rabbit-admin-body.json" -ErrorAction SilentlyContinue
Write-Host ""
