query=$(curl -H "Accept:text/sparql" https://databus.dbpedia.org/dbpedia/collections/latest-core)
files=$(curl -H "Accept: text/csv" --data-urlencode "query=${query}" https://databus.dbpedia.org/repo/sparql | tail -n+2 | sed 's/"//g')
while IFS= read -r file ; do wget $file; done <<< "$files"