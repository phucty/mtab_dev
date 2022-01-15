wp='20211101'
wd1='20211101'
wd2='20210628'

# "https://dumps.wikimedia.org/wikidatawiki/entities/${wd2}/wikidata-${wd2}-all.json.bz2"

declare -a items=(
"https://dumps.wikimedia.org/wikidatawiki/${wd1}/wikidatawiki-${wd1}-page.sql.gz" 
"https://dumps.wikimedia.org/wikidatawiki/${wd1}/wikidatawiki-${wd1}-redirect.sql.gz" 
"https://dumps.wikimedia.org/enwiki/${wp}/enwiki-${wp}-redirect.sql.gz"
"https://dumps.wikimedia.org/enwiki/${wp}/enwiki-${wp}-pages-articles.xml.bz2"
"https://dumps.wikimedia.org/enwiki/${wp}/enwiki-${wp}-page.sql.gz"
"https://dumps.wikimedia.org/enwiki/${wp}/enwiki-${wp}-page_props.sql.gz"
)


for i in "${items[@]}"
do
	echo "Downloading $i:"
	wget -c "$i"
done



