Write-Host "Setting parameters..."

# edit these parameters before running the script
$name="functionssb6"
$location="westus"
$storageSku="Standard_LRS"

if (($name -eq "unique-alphanumeric-name-no-dashes")) 
{
	Write-Error "You have to edit this script: please insert valid parameter values."
	exit
}
if (($Env:EventHubsConnection -eq $null) -or ($Env:EventHubsConnection -eq "")) 
{
	Write-Error "You have to set the environment variable EventHubsConnection to use this script."
	exit
}

# by default, use the same name for group, function app, storage account, and plan
$groupName=$name
$functionAppName=$name
$storageName=$name

Write-Host "Creating Resource Group..."
az group create --name $groupName --location $location

Write-Host "Creating Storage Account..."
az storage account create --name  $storageName --location $location --resource-group  $groupName --sku $storageSku

Write-Host "Creating and Configuring Function App..."
az functionapp create --functions-version 3 --name  $functionAppName --storage-account $storageName --consumption-plan-location $location --resource-group $groupName
az functionapp config appsettings set -n $functionAppName -g  $groupName --settings EventHubsConnection=$Env:EventHubsConnection
#az functionapp config set -n $functionAppName -g $groupName --use-32bit-worker-process false

Write-Host "Publishing Code to Function App..."
func azure functionapp publish $functionAppName
