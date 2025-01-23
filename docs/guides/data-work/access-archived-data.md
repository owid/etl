---
status: new
---

Sometimes, you may need to access archived datasets or snapshots and compare them with current datasets. Here are the recommended approaches:

---

### Going Back in Git History

The simplest way to access older datasets is by checking out a previous Git commit and running the ETL process from that point in time.

1. **Find the commit of interest**:
   - Open the file in GitHub.
   - Click the `History` button.
   - Select the desired commit and copy its SHA (click the copy button).

2. **Checkout the commit**:
   ```bash
   git checkout <SHA>
   ```

3. **Re-run the ETL**:
   ```bash
   make .venv
   etlr <dataset>
   ```

💡 **Tip**: Run this in a separate folder (e.g., `etl2`) to retain access to the current datasets. This setup allows you to compare datasets in a notebook.

Example comparison in Python:
```python
from etl.dataset import Dataset

# Load current dataset
tb_current = Dataset("~/projects/etl/data/garden/climate/latest/weekly_wildfires").read_table('wildfires')

# Load dataset from a previous commit
tb_old = Dataset("~/projects/etl2/data/garden/climate/latest/weekly_wildfires").read_table('wildfires')
```

---

### Updating Snapshot MD5 for Archived Snapshots

If the code hasn’t changed and only new snapshots have been created (e.g., for automatically updated datasets), you can modify the snapshot MD5 in the `.dvc` file to point to an older snapshot.

1. **Find the MD5 and size**:
   - Locate the desired commit in GitHub.
   - Copy the MD5 and size from the relevant `.dvc` file (e.g., `snapshots/climate/latest/weekly_wildfires.csv.dvc`).

2. **Update the `.dvc` file locally**:
   - Replace the MD5 and size in your local `.dvc` file.

3. **Re-run the ETL** with the updated MD5:
   ```bash
   make .venv
   etlr <dataset>
   ```

💡 **Tip**: For chart comparisons, create a PR with the updated `.dvc` file, commit the changes, and use the chart diff tool. Enable "Show all charts" to view them side-by-side.

---

### Comparing Snapshots

To directly compare snapshots, use the `etl.snapshot` module.

1. **Load the current snapshot**:
   ```python
   from etl.snapshot import Snapshot

   snap = Snapshot("climate/latest/weekly_wildfires.csv")
   snap.pull()
   pd.read_csv(snap.path).shape
   ```

2. **Load an older snapshot**:
   - Find its MD5 and size from a previous commit.
   - Update the MD5 and size in your script:
     ```python
     from etl.snapshot import Snapshot

     snap = Snapshot("climate/latest/weekly_wildfires.csv")
     snap.metadata.outs[0]["md5"] = "356177e363926b959f5af281443f0a35"
     snap.metadata.outs[0]["size"] = 12548867
     snap.pull()
     pd.read_csv(snap.path).shape
     ```

---
