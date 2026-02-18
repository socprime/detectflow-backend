from uuid import NAMESPACE_DNS, uuid5

# Hardcoded list of external repositories
# Using UUID5 to generate deterministic UUIDs based on repository names
SIGMAHQ_UUID = str(uuid5(NAMESPACE_DNS, "sigmahq"))
SIGMAHQ_URL = "https://github.com/SigmaHQ/sigma"
SPLUNK_GITHUB_UUID = str(uuid5(NAMESPACE_DNS, "splunk-github"))
SPLUNK_GITHUB_URL = "https://github.com/splunk/security_content/tree/develop/detections"
ELASTIC_GITHUB_UUID = str(uuid5(NAMESPACE_DNS, "elastic-github"))
ELASTIC_GITHUB_URL = "https://github.com/elastic/detection-rules/tree/main/rules"
MICROSOFT_AZURE_SENTINEL_GITHUB_UUID = str(uuid5(NAMESPACE_DNS, "microsoft-azure-sentinel-github"))
MICROSOFT_AZURE_SENTINEL_GITHUB_URL = "https://github.com/Azure/Azure-Sentinel/tree/master/Hunting%20Queries"
EXTERNAL_REPOSITORIES = [
    {
        "id": SIGMAHQ_UUID,
        "name": "SigmaHQ",
        "source_link": SIGMAHQ_URL,
    },
    {
        "id": SPLUNK_GITHUB_UUID,
        "name": "Splunk (GitHub)",
        "source_link": SPLUNK_GITHUB_URL,
    },
    {
        "id": ELASTIC_GITHUB_UUID,
        "name": "Elastic (GitHub)",
        "source_link": ELASTIC_GITHUB_URL,
    },
    {
        "id": MICROSOFT_AZURE_SENTINEL_GITHUB_UUID,
        "name": "Microsoft Azure Sentinel (GitHub)",
        "source_link": MICROSOFT_AZURE_SENTINEL_GITHUB_URL,
    },
]
