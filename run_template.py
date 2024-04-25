from mongodb_generic_mirroring import mirror

conn_str = "YOUR_MONGODB_CONN_STR"
db_name = "YOUR_MONGODB_DB_NAME"
coll_name = "YOUR_MONGODB_COLLECTION_NAME"
# this url can be obtained in Azure Storage Explorer by going to:
# the L0 mirroring service you created
# -> Files
# -> LandingZone (create if not exists already)
# -> right click and choose Copy URL -> With DFS Endpoint
lz_url = "YOUR_LANDING_ZONE_URL"

# You also need APP_ID, SECRET and TENANT_ID from your Azure Registered App in .env file
mirror(conn_str, db_name, coll_name, lz_url)