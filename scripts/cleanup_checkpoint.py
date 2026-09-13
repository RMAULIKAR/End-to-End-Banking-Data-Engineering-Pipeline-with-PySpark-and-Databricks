BASE_PATH = "/Volumes/bankingprojectpyspark_cata/man_schema/project_storage"

dbutils.fs.rm(
    f"{BASE_PATH}/checkpoints",
    True
)

print("Checkpoints folder cleared.")