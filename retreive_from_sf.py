from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import snowflake.connector


# global variable
snowflake_acc = '<account_name>'

credential = DefaultAzureCredential()

secret_client = SecretClient(vault_url="https://<keyvault_name>.vault.azure.net/", credential=credential)
username = secret_client.get_secret("<snowflake-technical-user-secret>")
pass_sf = secret_client.get_secret('snowflake-technical-pass-secret')
print('{} is {}'.format(username.name,username.value))
print('{} is {}'.format(pass_sf.name,pass_sf.value))

ctx = snowflake.connector.connect(
    user=username.value,
    password=pass_sf.value,
    account= snowflake_acc
    )
cs = ctx.cursor()
try:
    cs.execute("Use warehouse DEMO_WH")
    cs.execute("Use DEMO_DB")
    cs.execute("Use schema PUBLIC;")
    cs.execute("SELECT * From PYTHONDEMOTABLE")
    # one_row = cs.fetchone()
    # print(one_row)
    for row in cs:
        print(row)
finally:
    cs.close()

