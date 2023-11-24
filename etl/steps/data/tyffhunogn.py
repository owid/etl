
from owid.catalog import Dataset
def run(dest_dir):
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "test"
    ds.save()
            
