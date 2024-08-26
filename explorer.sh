echo "setting owid-content"
cd ../owid-content/
git checkout covid-explorer

echo "running ETL"
cd ../etl/
etlr covid/latest/covid --private --explorer --force --only

echo "Commiting & pushing new files"
cd ../owid-content/
git add explorers/covid.explorer.tsv
git commit -m "wip: explorer"
git push origin covid-explorer
