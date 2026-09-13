BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

folders_to_clean = [
    f"{BASE_PATH}/bronze",
    f"{BASE_PATH}/silver",
    f"{BASE_PATH}/gold"
]

for folder in folders_to_clean:
    try:
        for item in dbutils.fs.ls(folder):
            dbutils.fs.rm(item.path, True)
            print(f"Deleted: {item.path}")

    except Exception as e:
        print(f"Could not clean {folder}: {e}")

print("Pipeline data cleanup completed!")